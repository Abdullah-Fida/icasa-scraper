import urllib.request
import json
import ssl
import time
import argparse
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

API_ENDPOINT = "https://api.we-net.ch/api/listings/check-url"


def load_urls(path):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [l.strip() for l in f if l.strip()]


def read_existing_processed(path):
    seen = set()
    if not os.path.exists(path):
        return seen
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    obj = json.loads(line)
                    url = obj.get("url")
                    if url:
                        seen.add(url)
                except Exception:
                    continue
    except Exception:
        pass
    return seen


def stream_post_check(urls, out_path, delay=0.1, resume=False, ssl_ctx=None, prefix_type=None, workers=1):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    processed = read_existing_processed(out_path) if resume else set()
    # filter out already processed URLs when resuming
    to_process = [u for u in urls if u not in processed]
    total = len(to_process)
    if total == 0:
        print("No new URLs to process.")
        return 0

    # Serial path (workers == 1)
    if workers <= 1:
        done = 0
        with open(out_path, "a", encoding="utf-8") as outf:
            for i, url in enumerate(to_process, start=1):
                payload = json.dumps({"detail_url": url}).encode("utf-8")
                req = urllib.request.Request(API_ENDPOINT, data=payload, headers={"Content-Type": "application/json"}, method="POST")
                try:
                    with urllib.request.urlopen(req, context=ssl_ctx, timeout=30) as resp:
                        resp_text = resp.read().decode("utf-8")
                        try:
                            resp_json = json.loads(resp_text)
                        except Exception:
                            resp_json = resp_text
                        entry = {"url": url, "status": getattr(resp, "status", None), "response": resp_json}
                        if prefix_type:
                            entry["type"] = prefix_type
                        outf.write(json.dumps(entry, ensure_ascii=False) + "\n")
                        outf.flush()
                        print(f"[{i}/{total}] OK {url} -> {getattr(resp, 'status', '-')}")
                except urllib.error.HTTPError as e:
                    try:
                        details = e.read().decode("utf-8")
                    except Exception:
                        details = ""
                    entry = {"url": url, "status": e.code, "error": e.reason, "details": details}
                    if prefix_type:
                        entry["type"] = prefix_type
                    outf.write(json.dumps(entry, ensure_ascii=False) + "\n")
                    outf.flush()
                    print(f"[{i}/{total}] HTTPError {e.code} {url}")
                except Exception as e:
                    entry = {"url": url, "error": str(e)}
                    if prefix_type:
                        entry["type"] = prefix_type
                    outf.write(json.dumps(entry, ensure_ascii=False) + "\n")
                    outf.flush()
                    print(f"[{i}/{total}] Exception for {url}: {e}")

                done += 1
                if delay:
                    time.sleep(delay)

        return done

    # Concurrent path
    write_lock = threading.Lock()
    def post_single(url, idx):
        payload = json.dumps({"detail_url": url}).encode("utf-8")
        req = urllib.request.Request(API_ENDPOINT, data=payload, headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, context=ssl_ctx, timeout=30) as resp:
                resp_text = resp.read().decode("utf-8")
                try:
                    resp_json = json.loads(resp_text)
                except Exception:
                    resp_json = resp_text
                entry = {"url": url, "status": getattr(resp, "status", None), "response": resp_json}
                if prefix_type:
                    entry["type"] = prefix_type
                with write_lock:
                    outf.write(json.dumps(entry, ensure_ascii=False) + "\n")
                    outf.flush()
                return getattr(resp, "status", None)
        except urllib.error.HTTPError as e:
            try:
                details = e.read().decode("utf-8")
            except Exception:
                details = ""
            entry = {"url": url, "status": e.code, "error": e.reason, "details": details}
            if prefix_type:
                entry["type"] = prefix_type
            with write_lock:
                outf.write(json.dumps(entry, ensure_ascii=False) + "\n")
                outf.flush()
            return e.code
        except Exception as e:
            entry = {"url": url, "error": str(e)}
            if prefix_type:
                entry["type"] = prefix_type
            with write_lock:
                outf.write(json.dumps(entry, ensure_ascii=False) + "\n")
                outf.flush()
            return None

    done = 0
    with open(out_path, "a", encoding="utf-8") as outf:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(post_single, url, idx): url for idx, url in enumerate(to_process, start=1)}
            for future in as_completed(futures):
                url = futures[future]
                done += 1
                try:
                    status = future.result()
                    print(f"[{done}/{total}] {url} -> {status}")
                except Exception as e:
                    print(f"[{done}/{total}] Exception for {url}: {e}")
                if delay:
                    # small pause between handling completions to yield CPU and avoid bursts
                    time.sleep(delay)

    return done


def convert_jsonl_to_json(jsonl_path, json_path):
    if not os.path.exists(jsonl_path):
        print("No jsonl file to convert.")
        return
    with open(jsonl_path, "r", encoding="utf-8") as inf, open(json_path, "w", encoding="utf-8") as outf:
        outf.write("[")
        first = True
        for line in inf:
            line = line.strip()
            if not line:
                continue
            if not first:
                outf.write(",\n")
            outf.write(line)
            first = False
        outf.write("]\n")


def main():
    parser = argparse.ArgumentParser(description="Check status of buy/rent URLs via API and stream results.")
    parser.add_argument("--buy", default="output/buy_urls.txt", help="Path to buy URLs file")
    parser.add_argument("--rent", default="output/rent_urls.txt", help="Path to rent URLs file")
    parser.add_argument("--out", default="output/results.jsonl", help="Path to output jsonl file")
    parser.add_argument("--json", default="output/result.json", help="Path to final JSON file (array) to produce")
    parser.add_argument("--delay", type=float, default=0.1, help="Delay between requests (seconds)")
    parser.add_argument("--resume", action="store_true", help="Resume from existing output file if present")
    parser.add_argument("--workers", type=int, default=1, help="Number of parallel worker threads")
    parser.add_argument("--only", choices=["buy","rent","both"], default="both", help="Which lists to check")
    args = parser.parse_args()

    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE

    urls = []
    if args.only in ("both","buy"):
        buy_list = load_urls(args.buy)
        print(f"Loaded {len(buy_list)} buy URLs from {args.buy}")
        if buy_list:
            urls.append(("buy", buy_list))

    if args.only in ("both","rent"):
        rent_list = load_urls(args.rent)
        print(f"Loaded {len(rent_list)} rent URLs from {args.rent}")
        if rent_list:
            urls.append(("rent", rent_list))

    if not urls:
        print("No URLs to process. Check your input files.")
        sys.exit(1)

    total_done = 0
    for prefix, lst in urls:
        print(f"Processing {len(lst)} {prefix} URLs...")
        total_done += stream_post_check(lst, args.out, delay=args.delay, resume=args.resume, ssl_ctx=ssl_ctx, prefix_type=prefix, workers=args.workers)

    print(f"Finished checks. Total processed (this run): {total_done}")
    print(f"Converting {args.out} -> {args.json} (streaming)...")
    convert_jsonl_to_json(args.out, args.json)
    print("Done.")


if __name__ == "__main__":
    main()
