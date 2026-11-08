from pathlib import Path, PurePosixPath


def inside_storage(root, candidate, remote=False):
    if not root or not candidate or "\x00" in candidate or "\\" in candidate:
        return False
    # Reject traversal even before resolving a path; remote paths cannot be realpath'd locally.
    if ".." in PurePosixPath(candidate).parts or ".." in PurePosixPath(root).parts:
        return False
    if remote:
        a, b = PurePosixPath(root), PurePosixPath(candidate)
        if not a.is_absolute() or not b.is_absolute():
            return False
    else:
        a, b = Path(root).resolve(), Path(candidate).resolve()
    return a != b and a in b.parents
