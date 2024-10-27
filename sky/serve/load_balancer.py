"""LoadBalancer: Distribute any incoming request to all ready replicas."""
import asyncio
import logging
import os
import threading
import tempfile
from typing import Dict, List, Optional, Union
from urllib.parse import urlparse

import aiohttp
import fastapi
import httpx
from starlette import background
import uvicorn
import yaml

from sky import sky_logging
from sky.serve import constants
from sky.serve import load_balancing_policies as lb_policies
from sky.serve import serve_utils
from sky.utils import common_utils

logger = sky_logging.init_logger(__name__)

# TODO(ejj): Major things that may be needed before merging Envoy Support.
# This comment should go away before merging.
#
# - Automated Unit/Smoke Testing: Obviously this needs to be implemented before
# merging.  For now the code was tested by hand.
#
# - Autoscaling:  PythonLoadBalancer uses a list of each request and the
# time it was handled to calculate a qps number which the controller uses to
# make autoscaling decisions.  Envoy doesn't export stats with this level of
# detail, so we will need to find a different approach.  A couple of
# considerations:
#    - We could simulate the existing functionality by simply asking Envoy how
#    many requests it has handled for each replica on some regular cadence, and
#    using that to calculate an estimated qps number.   This should be
#    relatively straight forward to implement.
#    - We should consider, however, whether qps is really the right way to be
#    making auto-scaling decisions.  For AI serving workloads, does qps really
#    tell you how loaded the replica is?   In the non-AI world, one would
#    generally use CPU/Memory pressure for this purpose, however there are some
#    challenges here with regard to measuring load on GPUs.  This requires
#    further discussion.
#
# - Kubernetes Support:  The PR relies on using the `docker` command line to
# boot an Envoy container per service.  This won't work in Kubernetes where the
# sky controller is a pod, and thus can't run sub-containers.   There are two
# ways this could be handled.  1) Run envoy as a process instead of a docker
# container.  2) Switch to single-envoy per controller, and boot that Envoy in
# the pod-spec of the Kubernetes controller.


# Actual things to do:
# - Unit tests
# - Documentation
#   - Including what is and is not supported.
        # - Kubernetes doesn't work
        # - qps
# - Fail if running on Kubernetes
# - Document what's not supported


class SkyServeLoadBalancer:
    """SkyServeLoadBalancer: load balancer for distributing requests to Sky
    Serve replicas.

    The SkyServeLoadBalancer class serves as the base class for the the various
    child implementations.
    """

    def __init__(self, service_name: str, controller_url: str,
                 load_balancer_port: int) -> None:
        """Initialize the load balancer.

        Args:
            service_name: The name of the service this load balancer serves.
            controller_url: The URL of the controller.
            load_balancer_port: The port where the load balancer listens to.
        """

        self._controller_url = controller_url
        self._load_balancer_port = load_balancer_port
        self._service_name = service_name

    async def _controller_sync(self,
                               request_aggregator: dict) -> Optional[List[str]]:
        """ Sync with the controller once.

        Contact the controller. Give it the information contained in
        `request_aggregator`.  Receive the current set of Available replicas.
        """

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                        self._controller_url + '/controller/load_balancer_sync',
                        json={'request_aggregator': request_aggregator},
                        timeout=aiohttp.ClientTimeout(5)) as response:

                    response.raise_for_status()
                    response_json = await response.json()
                    ready_replica_urls = response_json.get(
                        'ready_replica_urls', [])
        except aiohttp.ClientError as e:
            logger.error('An error occurred when syncing with '
                         f'the controller: {e}')
            return None
        else:
            logger.debug(f'Available Replica URLs: {ready_replica_urls}')
            return ready_replica_urls


class PythonLoadBalancer(SkyServeLoadBalancer):
    """SkyServeLoadBalancer: distribute incoming traffic with proxy.

    This class accept any traffic to the controller and proxies it
    to the appropriate endpoint replica according to the load balancing
    policy.
    """

    def __init__(self,
                 service_name: str,
                 controller_url: str,
                 load_balancer_port: int,
                 load_balancing_policy_name: Optional[str] = None) -> None:
        """Initialize the load balancer.

        Args:
            service_name: The name of the service this load balancer serves.
            controller_url: The URL of the controller.
            load_balancer_port: The port where the load balancer listens to.
            load_balancing_policy_name: The name of the load balancing policy
                to use. Defaults to None.
        """
        super().__init__(service_name, controller_url, load_balancer_port)
        self._app = fastapi.FastAPI()
        self._controller_url: str = controller_url
        self._load_balancer_port: int = load_balancer_port
        # Use the registry to create the load balancing policy
        self._load_balancing_policy = lb_policies.LoadBalancingPolicy.make(
            load_balancing_policy_name)
        self._request_aggregator: serve_utils.RequestsAggregator = (
            serve_utils.RequestTimestamp())
        # TODO(tian): httpx.Client has a resource limit of 100 max connections
        # for each client. We should wait for feedback on the best max
        # connections.
        # Reference: https://www.python-httpx.org/advanced/resource-limits/
        #
        # If more than 100 requests are sent to the same replica, the
        # httpx.Client will queue the requests and send them when a
        # connection is available.
        # Reference: https://github.com/encode/httpcore/blob/a8f80980daaca98d556baea1783c5568775daadc/httpcore/_async/connection_pool.py#L69-L71 # pylint: disable=line-too-long
        self._client_pool: Dict[str, httpx.AsyncClient] = dict()
        # We need this lock to avoid getting from the client pool while
        # updating it from _sync_with_controller.
        self._client_pool_lock: threading.Lock = threading.Lock()

    async def _sync_with_controller(self):
        """Sync with controller periodically.

        Every `constants.LB_CONTROLLER_SYNC_INTERVAL_SECONDS` seconds, the
        load balancer will sync with the controller to get the latest
        information about available replicas; also, it report the request
        information to the controller, so that the controller can make
        autoscaling decisions.
        """
        # Sleep for a while to wait the controller bootstrap.
        await asyncio.sleep(5)

        while True:
            close_client_tasks = []

            request_aggregator = self._request_aggregator.to_dict()
            # Clean up before _controller_sync() early avoid OOM.
            self._request_aggregator.clear()

            ready_replica_urls = await self._controller_sync(request_aggregator)
            if ready_replica_urls is not None:
                with self._client_pool_lock:
                    self._load_balancing_policy.set_ready_replicas(
                        ready_replica_urls)
                    for replica_url in ready_replica_urls:
                        if replica_url not in self._client_pool:
                            self._client_pool[replica_url] = (httpx.AsyncClient(
                                base_url=replica_url))
                    urls_to_close = set(
                        self._client_pool.keys()) - set(ready_replica_urls)
                    client_to_close = []

                    for replica_url in urls_to_close:
                        client_to_close.append(
                            self._client_pool.pop(replica_url))

                    for client in client_to_close:
                        close_client_tasks.append(client.aclose())

            await asyncio.sleep(constants.LB_CONTROLLER_SYNC_INTERVAL_SECONDS)
            # Await those tasks after the interval to avoid blocking.
            await asyncio.gather(*close_client_tasks)

    async def _proxy_request_to(
        self, url: str, request: fastapi.Request
    ) -> Union[fastapi.responses.Response, Exception]:
        """Proxy the request to the specified URL.

        Returns:
            The response from the endpoint replica. Return the exception
            encountered if anything goes wrong.
        """
        logger.info(f'Proxy request to {url}')
        try:
            # We defer the get of the client here on purpose, for case when the
            # replica is ready in `_proxy_with_retries` but refreshed before
            # entering this function. In that case we will return an error here
            # and retry to find next ready replica. We also need to wait for the
            # update of the client pool to finish before getting the client.
            with self._client_pool_lock:
                client = self._client_pool.get(url, None)
            if client is None:
                return RuntimeError(f'Client for {url} not found.')
            worker_url = httpx.URL(path=request.url.path,
                                   query=request.url.query.encode('utf-8'))
            proxy_request = client.build_request(
                request.method,
                worker_url,
                headers=request.headers.raw,
                content=await request.body(),
                timeout=constants.LB_STREAM_TIMEOUT)
            proxy_response = await client.send(proxy_request, stream=True)
            return fastapi.responses.StreamingResponse(
                content=proxy_response.aiter_raw(),
                status_code=proxy_response.status_code,
                headers=proxy_response.headers,
                background=background.BackgroundTask(proxy_response.aclose))
        except (httpx.RequestError, httpx.HTTPStatusError) as e:
            logger.error(f'Error when proxy request to {url}: '
                         f'{common_utils.format_exception(e)}')
            return e

    async def _proxy_with_retries(
            self, request: fastapi.Request) -> fastapi.responses.Response:
        """Try to proxy the request to the endpoint replica with retries."""
        self._request_aggregator.add(request)
        # TODO(tian): Finetune backoff parameters.
        backoff = common_utils.Backoff(initial_backoff=1)
        # SkyServe supports serving on Spot Instances. To avoid preemptions
        # during request handling, we add a retry here.
        retry_cnt = 0
        while True:
            retry_cnt += 1
            with self._client_pool_lock:
                ready_replica_url = self._load_balancing_policy.select_replica(
                    request)
            if ready_replica_url is None:
                response_or_exception = fastapi.HTTPException(
                    # 503 means that the server is currently
                    # unable to handle the incoming requests.
                    status_code=503,
                    detail='No ready replicas. '
                    'Use "sky serve status [SERVICE_NAME]" '
                    'to check the replica status.')
            else:
                response_or_exception = await self._proxy_request_to(
                    ready_replica_url, request)
            if not isinstance(response_or_exception, Exception):
                return response_or_exception
            # When the user aborts the request during streaming, the request
            # will be disconnected. We do not need to retry for this case.
            if await request.is_disconnected():
                # 499 means a client terminates the connection
                # before the server is able to respond.
                return fastapi.responses.Response(status_code=499)
            # TODO(tian): Fail fast for errors like 404 not found.
            if retry_cnt == constants.LB_MAX_RETRY:
                if isinstance(response_or_exception, fastapi.HTTPException):
                    raise response_or_exception
                exception = common_utils.remove_color(
                    common_utils.format_exception(response_or_exception,
                                                  use_bracket=True))
                raise fastapi.HTTPException(
                    # 500 means internal server error.
                    status_code=500,
                    detail=f'Max retries {constants.LB_MAX_RETRY} exceeded. '
                    f'Last error encountered: {exception}. Please use '
                    '"sky serve logs [SERVICE_NAME] --load-balancer" '
                    'for more information.')
            current_backoff = backoff.current_backoff()
            logger.error(f'Retry in {current_backoff} seconds.')
            await asyncio.sleep(current_backoff)

    def run(self):
        self._app.add_api_route('/{path:path}',
                                self._proxy_with_retries,
                                methods=['GET', 'POST', 'PUT', 'DELETE'])

        @self._app.on_event('startup')
        async def startup():
            # Configure logger
            uvicorn_access_logger = logging.getLogger('uvicorn.access')
            for handler in uvicorn_access_logger.handlers:
                handler.setFormatter(sky_logging.FORMATTER)

            # Register controller synchronization task
            asyncio.create_task(self._sync_with_controller())

        logger.info('SkyServe Load Balancer started on '
                    f'http://0.0.0.0:{self._load_balancer_port}')

        uvicorn.run(self._app, host='0.0.0.0', port=self._load_balancer_port)


def run_load_balancer(service_name: str,
                      controller_addr: str,
                      load_balancer_port: int,
                      load_balancer_type: Optional[str] = None,
                      load_balancing_policy_name: Optional[str] = None) -> None:
    """ Run the load balancer.

    Args:
        controller_addr: The address of the controller.
        load_balancer_port: The port where the load balancer listens to.
        policy_name: The name of the load balancing policy to use. Defaults to
            None.
    """

    match load_balancer_type:
        case constants.LB_TYPE_PYTHON | None:
            plb = PythonLoadBalancer(
                service_name = service_name,
                controller_url=controller_addr,
                load_balancer_port=load_balancer_port,
                load_balancing_policy_name=load_balancing_policy_name)
            plb.run()
        case constants.LB_TYPE_ENVOY:
            elb = EnvoyLoadBalancer(service_name=service_name,
                                    controller_url=controller_addr,
                                    load_balancer_port=load_balancer_port)
            asyncio.run(elb.run())
        case _:
            raise ValueError('Unknown load balancer type:' +
                             ' {load_balanacer_type}')


class EnvoyLoadBalancer(SkyServeLoadBalancer):
    """ Envoy implementation of SkyServeLoadBalancer

    Envoy (https://www.envoyproxy.io/) is an Open Source HTTP proxy widely used
    for both north-south and east-west load balancing in cloud-native
    deployments.   The Envoy Sky load balancer instantiates an Envoy load
    balancer in a docker container, and configures to forward traffic
    appropriately to replicas using Envoy configuration files.  """

    def __init__(self, service_name: str, controller_url: str,
                 load_balancer_port: int) -> None:
        """ Initialize the Envoy load balancer

        Args:
            service_name: The name of the service this load balancer serves.
            controller_url: The URL of the controller.
            load_balancer_port: Ingress port for the load balancer.
        """

        super().__init__(service_name, controller_url, load_balancer_port)

        # Name of the Envoy container.
        self.envoy_name = EnvoyLoadBalancer._gen_envoy_name(service_name)

        # Folder which we will mount into the envoy docker container that will
        # container the Envoy config file
        self.envoy_config_folder = f'/home/ubuntu/envoy/{self.envoy_name}'

    @staticmethod
    def _gen_envoy_name(service_name: str) -> str:
        """Generate the name of an Envoy container from its service name."""
        return f'envoy-{service_name}'

    @staticmethod
    async def stop_envoy(service_name: str):
        """Stop the Envoy container corresponding to the provided service.

        Args:
            service_name: Name of the service whose Envoy we should stop.
        """
        name = EnvoyLoadBalancer._gen_envoy_name(service_name)
        proc = await asyncio.create_subprocess_exec('docker', 'rm', '-f', name)
        if await proc.wait() != 0:
            # Note this is expected when using the python load balancer.  We
            # always clean up in the spirit of defensiveness.
            logger.debug('Failed to remove envoy: %s', name)

    async def _start_envoy(self) -> bool:
        """Start the Envoy container

        Returns:
            True if successful, otherwise False.
        """

        cmd = ['docker', 'run', '-d', '--name', self.envoy_name, '--restart',
            'unless-stopped', '-v', f'{self.envoy_config_folder}:/etc/envoy',
            '-p', f'{self._load_balancer_port}:{self._load_balancer_port}',
            f'envoyproxy/envoy:v{constants.ENVOY_VERSION}',
            '--concurrency', constants.ENVOY_CPU,
            '-c', '/etc/envoy/envoy.yaml']
        proc = await asyncio.create_subprocess_exec(*cmd)
        logger.debug(f'Starting Envoy with command: {" ".join(cmd)}')
        ret = await proc.wait()
        return ret == 0

    def write_yaml(self, envoy_config: dict, filename: str):
        """ Writes an envoy configuration object to disk atomically.

        Args:
            envoy_config: A python object representing envoy configuration.
            This object will be coverted to yaml and written to disk.

            filename: The name of the file the yaml will be written to.  Note
            this is just the base filename not the full path.
        """

        # Envoy is constantly watching most xds files.  To avoid confusing
        # partial writes, it's better to udpate the configuration files
        # atomically by writing to a temporary file and replacing the original.
        envoy_yaml = yaml.dump(envoy_config, default_flow_style=False)
        with tempfile.NamedTemporaryFile(mode='w', delete=False,
                                         dir=self.envoy_config_folder) as f:
            f.write(envoy_yaml)
            temp_path = f.name

        # Allow anyone to read the file so Envoy has access.
        os.chmod(temp_path, 0o644)
        os.rename(temp_path, f'{self.envoy_config_folder}/{filename}')


    def write_static_xds(self):
        """ TODO comment

            Make sure to get across the idea that there's a static file which
            doesn't change, and then the eds which does change
        """

        # Filters describe what to do with a connection received by a listener.
        # This filter says the request should be handled by the cluster defined
        # below.
        filters = [{
            'name': 'envoy.filters.network.http_connection_manager',
            'typed_config': {
                '@type':
                    'type.googleapis.com/envoy.extensions.filters.' +
                    'network.http_connection_manager.v3.HttpConnectionManager',
                'stat_prefix': 'ingress_http',
                'http_filters': [{
                    'name': 'envoy.filters.http.router',
                    'typed_config': {
                        '@type': 'type.googleapis.com/envoy.extensions.' +
                                 'filters.http.router.v3.Router',

                        # We aren't using dynamic_stats, and Envoy recommends
                        # disabling them for profiling.
                        'dynamic_stats': False
                    }
                }],

                # We don't use random request ids, and Envoy recommends
                # disabling for profiling.
                'generate_request_id': False,
                'route_config': {
                    'virtual_hosts': [{
                        'name': 'local_service',
                        'domains': ['*'],
                        'routes': [{
                            'match': {
                                'prefix': '/'
                            },
                            'route': {
                                'cluster': 'cluster'
                            }
                        }]
                    }]
                }
            }
        }]

        # Listeners are the entry point to envoy.  This one handles all traffic
        # received on the specified port using the filters described above.
        # received on port 8080 and processes it with the above filters.
        listener = {
            'name': 'listener',
            'address': {
                'socket_address': {
                    'address': '0.0.0.0',
                    'port_value': self._load_balancer_port,
                }
            },
            'filter_chains': [{
                'filters': filters
            }]
        }

        # A cluster is usually a group of endpoints that can be load balanced
        # over.  This one says to find the list of endpoints in eds.yaml.
        cluster = {
            'name': 'cluster',
            'connect_timeout': '0.25s',
            'type': 'EDS',
            'lb_policy': 'ROUND_ROBIN',  # TODO(ejj) weighted least request
            'eds_cluster_config': {
                'eds_config': {
                    'path_config_source': {
                        'path': '/etc/envoy/eds.yaml'
                    }
                }
            }
        }

        config = {
            'node': {
                'id': 'controller',
                'cluster': self._service_name,
            },
            'static_resources': {
                'listeners': [listener],
                'clusters': [cluster],
            }
        }


        os.makedirs(self.envoy_config_folder, exist_ok=True)
        self.write_yaml(config, 'envoy.yaml')

    def write_eds(self, replicas: List[str]):
        """
        TODO big commend.  EDS Endpoint Discover Service.
        List the end points we can load balance over.
        """

        lb_endpoints = []
        for url in replicas:
            # TODO(ejj) when Envoy is the only load balancer choice, we should
            # just have the controller send us the IP and port as a tuple
            # rather than bundling them together in a url.
            parsed_url = urlparse(url)
            lb_endpoints.append({
                'endpoint': {
                    'address': {
                        'socket_address': {
                            'address': parsed_url.hostname,
                            'port_value': parsed_url.port,
                        }
                    }
                }
            })

        config = {
            'resources': {
                '@type': 'type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment',
                'cluster_name': 'cluster',
                'endpoints': {
                    'lb_endpoints': lb_endpoints
                }
            }

        }
        self.write_yaml(config, 'eds.yaml')


    async def run(self):
        self.write_static_xds()

        # Because docker can take some time to come up, we make multiple
        # attempts to start before giving up.
        envoy_started = False
        logger.info('Starting envoy %s', self.envoy_name)
        for _ in range(30):
            await asyncio.sleep(5)
            envoy_started = await self._start_envoy()
            if envoy_started:
                break

        if not envoy_started:
            error = f'Failed to start envoy {self.envoy_name}'
            logger.error(error)
            raise RuntimeError(error)

        while True:
            await asyncio.sleep(constants.LB_CONTROLLER_SYNC_INTERVAL_SECONDS)
            ready_replica_urls = await self._controller_sync({})
            if ready_replica_urls is not None:
                # If there are no replica, there could really be no replicas,
                # or there could be something else wrong.  Either way, it
                # doesn't hurt to leave the config unchanged.
                self.write_eds(ready_replica_urls)

def cleanup(service_name: str):
    asyncio.run(EnvoyLoadBalancer.stop_envoy(service_name))

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--controller-addr',
                        required=True,
                        default='127.0.0.1',
                        help='The address of the controller.')
    parser.add_argument('--load-balancer-port',
                        type=int,
                        required=True,
                        default=8890,
                        help='The port where the load balancer listens to.')
    available_policies = list(lb_policies.LB_POLICIES.keys())
    parser.add_argument(
        '--load-balancing-policy',
        choices=available_policies,
        default='round_robin',
        help=f'The load balancing policy to use. Available policies: '
        f'{", ".join(available_policies)}.')
    args = parser.parse_args()
    run_load_balancer('cmd', args.controller_addr, args.load_balancer_port,
                      args.load_balanciong_policy)
