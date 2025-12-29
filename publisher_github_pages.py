import base64
import requests

def _gh_headers(token: str):
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

def get_file_sha(owner: str, repo: str, path: str, token: str, branch: str = "main"):
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"
    r = requests.get(url, headers=_gh_headers(token), params={"ref": branch}, timeout=30)
    if r.status_code == 200:
        return r.json().get("sha")
    if r.status_code == 404:
        return None
    r.raise_for_status()

def upsert_file(owner: str, repo: str, path: str, token: str, content_bytes: bytes,
                message: str, branch: str = "main"):
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"
    sha = get_file_sha(owner, repo, path, token, branch=branch)

    payload = {
        "message": message,
        "content": base64.b64encode(content_bytes).decode("utf-8"),
        "branch": branch,
    }
    if sha:
        payload["sha"] = sha

    r = requests.put(url, headers=_gh_headers(token), json=payload, timeout=30)
    if r.status_code not in (200, 201):
        try:
            detail = r.json()
        except Exception:
            detail = r.text
        raise RuntimeError(f"GitHub upload failed: {r.status_code} {detail}")
    return r.json()

def publish_html(owner: str, repo: str, token: str, html_str: str,
                 branch: str = "main", target_path: str = "index.html"):
    return upsert_file(
        owner=owner,
        repo=repo,
        path=target_path,
        token=token,
        content_bytes=html_str.encode("utf-8"),
        message=f"Update {target_path} (auto)",
        branch=branch,
    )
