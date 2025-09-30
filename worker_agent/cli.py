from __future__ import annotations

import socket

from .agent import WorkerAgent, build_arg_parser, configure_logging


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    worker_id = args.worker_id or socket.gethostname()
    configure_logging(args.log_level)

    agent = WorkerAgent(
        server_url=args.server,
        worker_id=worker_id,
        shared_dir=args.shared_dir,
        image=args.image,
        poll_interval=args.poll_interval,
        heartbeat_interval=args.heartbeat_interval,
        status_poll_interval=args.status_poll_interval,
    )
    agent.run_forever()


if __name__ == "__main__":  # pragma: no cover
    main()
