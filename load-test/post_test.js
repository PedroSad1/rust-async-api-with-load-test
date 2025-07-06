import http from "k6/http";
import { check } from "k6";

// RUN: k6 run --out influxdb=http://localhost:8086/k6 post_test.js

function uuidv4() {
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, function (c) {
    const r = (Math.random() * 16) | 0,
      v = c === "x" ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

export let options = {
  vus: 100,
  duration: "30s",
};

export default function () {
  const url = "http://localhost:8080/payments";
  const correlation_id = uuidv4();

  const payload = JSON.stringify({
    correlation_id: correlation_id,
    amount: Math.random() * 1000,
  });

  const params = {
    headers: {
      "Content-Type": "application/json",
    },
  };

  const res = http.post(url, payload, params);

  check(res, {
    "status is 201": (r) => r.status === 201,
  });
}
