const BACKEND_URL = process.env.BACKEND_API_URL || "http://127.0.0.1:8001";

async function proxy(request, context) {
  const params = await context.params;
  const path = (params?.path || []).join("/");
  const query = request.nextUrl.search || "";
  const url = `${BACKEND_URL}/api/v1/${path}${query}`;

  const init = {
    method: request.method,
    headers: request.headers,
    cache: "no-store",
  };

  if (request.method !== "GET" && request.method !== "HEAD") {
    init.body = await request.text();
  }

  const upstream = await fetch(url, init);
  const body = await upstream.text();

  return new Response(body, {
    status: upstream.status,
    headers: { "content-type": upstream.headers.get("content-type") || "application/json" },
  });
}

export async function GET(request, context) {
  return proxy(request, context);
}

export async function POST(request, context) {
  return proxy(request, context);
}

export async function PUT(request, context) {
  return proxy(request, context);
}

export async function PATCH(request, context) {
  return proxy(request, context);
}

export async function DELETE(request, context) {
  return proxy(request, context);
}
