import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  vus: 10,
  duration: "10s",
  thresholds: {
    http_req_failed: ["rate<0.1"],   // <10% error rate
    http_req_duration: ["p(95)<2000"], // 95th percentile < 2s
  },
};

const BASE_URL = __ENV.BASE_URL || "http://localhost";

export default function () {
  // Test GET /api/alerts (cached endpoint)
  const alertsRes = http.get(`${BASE_URL}/api/alerts`);
  check(alertsRes, {
    "alerts status 200": (r) => r.status === 200,
    "alerts has alerts field": (r) => {
      try {
        const body = JSON.parse(r.body);
        return body.hasOwnProperty("alerts");
      } catch {
        return false;
      }
    },
  });

  // Test health endpoints
  const ingestHealth = http.get(`${BASE_URL}/api/health/ingest`);
  check(ingestHealth, {
    "ingest health 200": (r) => r.status === 200,
  });

  const alertHealth = http.get(`${BASE_URL}/api/health/alert`);
  check(alertHealth, {
    "alert health 200": (r) => r.status === 200,
  });

  sleep(0.5);
}
