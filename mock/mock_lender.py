#!/usr/bin/env python3
"""Local stand-in for the Beeceptor mocks in README.md.

Serves both lenders (federal + demobank) on one port, with live scenario
flipping so you can drive the whole README scenario matrix without restarting
anything -- the workflow reacts because the branch conditions live in config.

  GET  /_state                  show current scenario knobs
  GET  /_set?key=val&key2=val2  flip a knob (see STATE below)
  GET  /_reset                  back to happy path
"""
import json, re
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

STATE = {
    "panStatusCode":        "E",      # E=pass, F/I=business reject, anything else=successWhen miss
    "panHttp":              "200",    # 500 => technical failure on the pan branch
    "unNameStatus":         "S",      # S=pass; anything else=successWhen miss
    "unNameHttp":           "200",    # 500 => technical failure on the un-name branch
    "panAadhaarHttp":       "200",
    "mobileAadhaarHttp":    "200",
    "aadhaarChecksNeeded":  "true",   # false => parallel fan-out 4 -> 2
    "precheckDedupeFlag":   "N",
    "dedupeFlag":           "N",      # Y => customerCreation + dedupeRetry SKIPPED
    "lenderCustomerId":     "",       # non-blank + dedupeFlag=Y => dedupeRetry skipped
    "loanStatusSucceedsAfter": "2",   # polls before /stp/loan-status flips to SUCCESS
    "ftStatusSucceedsAfter":   "2",   # polls before /ft/status flips to SUCCESS
    "dedupeHttp":           "200",    # set 500 => technical failure + backoff retries
    "accountInfoStatus":    "200",    # 406 => business reject
    "sanctionApprovesAfter":"2",      # demobank
}
COUNTERS = {"loan": 0, "ft": 0, "sanction": 0}

def _int(k, d=2):
    try: return int(STATE.get(k, d))
    except (TypeError, ValueError): return d

def route(method, path):
    s = STATE
    # ---------------- federal / PRECHECK
    if path == "/precheck/name-dob":
        return 200, {"status": "SUCCESS"}
    if path == "/precheck/dedupe":
        return 200, {"status": "SUCCESS", "data": {
            "ddupe_flag": s["precheckDedupeFlag"],
            "customer_id": "",
            "aadhaar_checks_needed": s["aadhaarChecksNeeded"]}}
    if path == "/precheck/pan":
        code = _int("panHttp", 200)
        if code >= 500:
            return code, {"error": "simulated pan outage"}
        return 200, {"panStatusCode": s["panStatusCode"], "status": "SUCCESS"}
    if path == "/precheck/un-name":
        code = _int("unNameHttp", 200)
        if code >= 500:
            return code, {"error": "simulated un-name outage"}
        return 200, {"status": s["unNameStatus"]}
    if path == "/precheck/pan-aadhaar":
        code = _int("panAadhaarHttp", 200)
        if code >= 500:
            return code, {"error": "simulated pan-aadhaar outage"}
        return 200, {"status": "SUCCESS"}
    if path == "/precheck/mobile-aadhaar":
        code = _int("mobileAadhaarHttp", 200)
        if code >= 500:
            return code, {"error": "simulated mobile-aadhaar outage"}
        return 200, {"status": "SUCCESS"}
    # ---------------- federal / STP
    if path == "/stp/dedupe":
        code = _int("dedupeHttp", 200)
        if code >= 500:
            return code, {"error": "simulated upstream failure"}
        return 200, {"ddupeFlag": s["dedupeFlag"], "partialKycFlag": "N",
                     "lenderCustomerId": s["lenderCustomerId"]}
    if path == "/stp/customer-creation":
        return 200, {"responseCode": "00", "customerId": "CIF-1001", "partialKycFlag": "N"}
    if path == "/stp/dedupe-retry":
        return 200, {"lenderCustomerId": "CIF-1001"}
    if path == "/stp/customer-info":
        return 200, {"status": "SUCCESS", "referenceNumber": "REF-1"}
    if path == "/stp/account-info":
        code = s["accountInfoStatus"]
        return 200, {"statusCode": code,
                     "status": "SUCCESS" if code == "200" else "REJECTED",
                     "loanAccountNumber": "LA-77001"}
    if path == "/stp/loan-status":
        COUNTERS["loan"] += 1
        n = _int("loanStatusSucceedsAfter")
        done = n >= 0 and COUNTERS["loan"] >= n
        print(f"    [mock] loan-status poll #{COUNTERS['loan']} -> "
              f"{'SUCCESS' if done else 'PENDING'}", flush=True)
        return 200, {"loanStatus": "SUCCESS" if done else "PENDING"}
    # ---------------- federal / FUND_TRANSFER
    if path == "/ft/pledge-card":
        return 200, {"statusCode": "200", "bpmNumber": "BPM-9001"}
    if path == "/ft/status":
        COUNTERS["ft"] += 1
        n = _int("ftStatusSucceedsAfter")
        done = n >= 0 and COUNTERS["ft"] >= n
        print(f"    [mock] ft-status poll #{COUNTERS['ft']} -> "
              f"{'SUCCESS' if done else 'PENDING'}", flush=True)
        if done:
            return 200, {"status": "SUCCESS", "utr": "UTR123456789"}
        return 200, {"status": "PENDING"}
    # ---------------- demobank (different graph, same JAR)
    if path == "/precheck":
        return 200, {"verdict": "PASS", "custRef": "DEMO-CUST-1"}
    if path == "/sanction/status":
        COUNTERS["sanction"] += 1
        n = _int("sanctionApprovesAfter")
        done = n >= 0 and COUNTERS["sanction"] >= n
        return 200, {"state": "APPROVED" if done else "PENDING"}
    if path == "/disburse":
        return 200, {"ok": "true", "utr": "UTR-DEMO-999"}
    return 404, {"error": "no mock for " + path}

class H(BaseHTTPRequestHandler):
    def _send(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle(self, method):
        u = urlparse(self.path)
        if u.path == "/_state":
            return self._send(200, {"state": STATE, "counters": COUNTERS})
        if u.path == "/_set":
            for k, v in parse_qs(u.query, keep_blank_values=True).items():
                if k in STATE:
                    STATE[k] = v[0]
                    print(f"    [mock] SET {k}={v[0]}", flush=True)
            COUNTERS.update(loan=0, ft=0, sanction=0)
            return self._send(200, {"state": STATE})
        if u.path == "/_reset":
            COUNTERS.update(loan=0, ft=0, sanction=0)
            return self._send(200, {"reset": True, "counters": COUNTERS})
        n = int(self.headers.get("Content-Length") or 0)
        if n:
            self.rfile.read(n)
        code, obj = route(method, u.path)
        print(f"  {method:4} {u.path:28} -> {code}", flush=True)
        self._send(code, obj)

    def do_GET(self):  self._handle("GET")
    def do_POST(self): self._handle("POST")
    def log_message(self, *a): pass

if __name__ == "__main__":
    print("lender mock on http://localhost:9099  (/_state /_set /_reset)", flush=True)
    HTTPServer(("127.0.0.1", 9099), H).serve_forever()
