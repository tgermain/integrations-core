# (C) Datadog, Inc. 2019-present
# All rights reserved
# Licensed under a 3-clause BSD style license (see LICENSE)
import os
import shutil
from contextlib import contextmanager

import pytest
from six import PY3

from .env import environment_run, get_state, save_state
from .structures import EnvVars, LazyFunction
from .subprocess import run_command
from .utils import find_check_root, get_check_name, get_here, get_tox_env, path_join

if PY3:
    from shutil import which
else:
    from shutilwhich import which


@contextmanager
def kind_run(directory, sleep=None, endpoints=None, conditions=None, env_vars=None, wrappers=None):
    """This utility provides a convenient way to safely set up and tear down Kind environments.

    :param directory: A path containing Kind files.
    :type directory: ``str``
    :param sleep: Number of seconds to wait before yielding.
    :type sleep: ``float``
    :param endpoints: Endpoints to verify access for before yielding. Shorthand for adding
                      ``conditions.CheckEndpoints(endpoints)`` to the ``conditions`` argument.
    :type endpoints: ``list`` of ``str``, or a single ``str``
    :param conditions: A list of callable objects that will be executed before yielding to check for errors.
    :type conditions: ``callable``
    :param env_vars: A dictionary to update ``os.environ`` with during execution.
    :type env_vars: ``dict``
    :param wrappers: A list of context managers to use during execution.
    """
    if not which('kind'):
        pytest.skip('Kind not available')

    get_here()
    set_up = KindUp(directory)
    tear_down = KindDown(directory)

    with environment_run(
        up=set_up,
        down=tear_down,
        sleep=sleep,
        endpoints=endpoints,
        conditions=conditions,
        env_vars=env_vars,
        wrappers=wrappers,
    ) as result:
        yield result


class KindUp(LazyFunction):
    """Create the kind cluster and use its context, calling
    `kind create cluster --name <integration>-cluster`

    It also returns the kubeconfig path as a `str`.
    """

    def __init__(self, directory):
        self.directory = directory
        self.check_root = find_check_root(depth=3)
        self.check_name = get_check_name(self.check_root)
        self.cluster_name = '{}-{}-cluster'.format(self.check_name, get_tox_env())

    def __call__(self):
        # Generated kubeconfig
        kube_path = path_join(self.check_root, '.kube')

        with EnvVars({'KUBECONFIG': path_join(kube_path, 'config')}):
            # Create cluster
            run_command(['kind', 'create', 'cluster', '--name', self.cluster_name], check=True)
            # Connect to cluster
            run_command(['kind', 'export', 'kubeconfig', '--name', self.cluster_name], check=True)

        # Move .kube/ to temp directory
        dst = shutil.move(kube_path, self.directory)

        # Temp kubeconfig
        tmp_path = path_join(dst, 'config')
        save_state('kubeconfig_path', tmp_path)

        return tmp_path


class KindDown(LazyFunction):
    """Delete the kind cluster, calling `delete cluster`."""

    def __init__(self, directory):
        self.directory = directory
        self.check_root = find_check_root(depth=3)
        self.check_name = get_check_name(self.check_root)
        self.cluster_name = '{}-{}-cluster'.format(self.check_name, get_tox_env())

    def __call__(self):
        kubeconfig_path = get_state('kubeconfig_path')

        with EnvVars({'KUBECONFIG': kubeconfig_path}):
            return run_command(['kind', 'delete', 'cluster', '--name', self.cluster_name], check=True)
