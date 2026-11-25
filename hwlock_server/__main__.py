"""python -m hwlock_server [--host H] [--port P] [--db PATH]   (other settings: HWLOCK_* env vars)"""

import argparse
import signal
import sys

from .web import App, Config


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="hwlock-server", description=__doc__)
    ap.add_argument("--host")
    ap.add_argument("--port", type=int)
    ap.add_argument("--db")
    a = ap.parse_args(argv)
    cfg = Config(**{k: v for k, v in vars(a).items() if v is not None})
    app = App(cfg)
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    try:
        app.serve_forever()
    except (KeyboardInterrupt, SystemExit):
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
