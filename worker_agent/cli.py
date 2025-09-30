from __future__ import annotations

import logging
import signal
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
        exit_after_job=args.exit_after_job,
    )

    logger = logging.getLogger(__name__)

    def _handle_exit_after_job(signum, frame):  # pragma: no cover - requires signal
        logger.info("Received %s; will terminate after current job", signal.Signals(signum).name)
        agent.request_exit_after_current_job()

    try:
        signal.signal(signal.SIGUSR1, _handle_exit_after_job)
    except AttributeError:  # pragma: no cover - platform without SIGUSR1
        logger.debug("SIGUSR1 not available on this platform; exit-after-job signal disabled")

    agent.run_forever()


if __name__ == "__main__":  # pragma: no cover
    main()
