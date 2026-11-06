"""Create offline evaluation, comparison and human decision artifacts."""
import argparse
import json
import os
from pathlib import Path
from .runner import CANDIDATES, compare, decide, evaluate


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def write(path, artifact):
    with Path(path).open("x", encoding="utf-8") as file:
        json.dump(artifact, file, indent=2, ensure_ascii=False)
        file.write("\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run")
    run.add_argument("--candidate", choices=CANDIDATES, required=True)
    run.add_argument("--output", required=True)
    run.add_argument("--suite", choices=("access-v1", "cross-v1"), default="access-v1")
    comparison = sub.add_parser("compare")
    comparison.add_argument("--baseline", required=True)
    comparison.add_argument("--candidate", required=True)
    comparison.add_argument("--output", required=True)
    decision = sub.add_parser("decide")
    decision.add_argument("--comparison", required=True)
    decision.add_argument("--decision", choices=["approved", "rejected", "needs_review"], required=True)
    decision.add_argument("--actor", required=True)
    decision.add_argument("--output", required=True)
    args = parser.parse_args()
    if args.command == "run":
        if args.suite == "cross-v1":
            from .cross import evaluate_cross
            result = evaluate_cross(os.environ["RUNTIME_DATABASE_URL"], {"access": os.environ["MOCK_IDENTITY_URL"],
                "notice": os.environ["MOCK_NOTICE_URL"]}, args.candidate)
        else:
            result = evaluate(os.environ["RUNTIME_DATABASE_URL"], os.environ["MOCK_IDENTITY_URL"], args.candidate)
    elif args.command == "compare":
        result = compare(read(args.baseline), read(args.candidate))
    else:
        result = decide(read(args.comparison), args.decision, args.actor)
    write(args.output, result)
    print(json.dumps({"kind": result["kind"], "id": result["id"], "output": args.output,
                      "counts": result.get("counts"), "total": result.get("total"),
                      "evidence_hash": result["evidence_hash"]}, indent=2))


if __name__ == "__main__":
    main()
