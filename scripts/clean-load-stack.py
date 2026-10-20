"""Clean only labeled disposable load resources, including workers left frozen by an interrupted test."""
from uuid import uuid4
from load_support import ROOT, Stack


if __name__ == "__main__":
    if (ROOT / ".loadtest.env").exists():
        stack = Stack(ROOT / "reports/phase5" / ("cleanup-" + str(uuid4())))
        stack.started = True
        stack.close()
        print("Removed labeled load-test containers and volumes; ordinary application data was untouched.")
