# ----------------------------------------------------------------------------
# Copyright (c) 2016-2023, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import concurrent.futures

from .base import Context


def _subprocess_apply(ctx, args, kwargs):
    exe = ctx.action_obj._bind(
        lambda: Context(ctx), {'type': 'asynchronous'})
    results = exe(*args, **kwargs)

    return results


class AsynchronousContext(Context):
    def _dispatch_(self, *args, **kwargs):
        # x-ref for why this try except exists
        # https://github.com/qiime2/qiime2/issues/73
        try:
            import matplotlib as plt
            if plt.rcParams['backend'].lower() == 'macosx':
                raise EnvironmentError(backend_error_template %
                                       plt.matplotlib_fname())
        except ImportError:
            pass

        # This function's signature is rewritten below using
        # `decorator.decorator`. When the signature is rewritten, args[0]
        # is the function whose signature was used to rewrite this
        # function's signature.
        args = args[1:]

        pool = concurrent.futures.ProcessPoolExecutor(max_workers=1)
        future = pool.submit(_subprocess_apply, self, args, kwargs)
        # TODO: pool.shutdown(wait=False) caused the child process to
        # hang unrecoverably. This seems to be a bug in Python 3.7
        # It's probably best to gut concurrent.futures entirely, so we're
        # ignoring the resource leakage for the moment.
        return future


# TODO add unit test for callables raising this
backend_error_template = """
Your current matplotlib backend (MacOSX) does not work with asynchronous calls.
A recommended backend is Agg, and can be changed by modifying your
matplotlibrc "backend" parameter, which can be found at: \n\n %s
"""
