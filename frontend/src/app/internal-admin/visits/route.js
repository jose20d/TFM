const BACKEND_URL = process.env.BACKEND_API_URL || "http://127.0.0.1:8001";
const INTERNAL_ADMIN_TOKEN = process.env.INTERNAL_ADMIN_TOKEN || "";

export async function GET(request) {
  if (!INTERNAL_ADMIN_TOKEN) {
    return new Response(
      JSON.stringify({ detail: "INTERNAL_ADMIN_TOKEN is not configured in frontend environment." }),
      { status: 503, headers: { "content-type": "application/json" } },
    );
  }

  const query = request.nextUrl.search || "";
  const upstream = await fetch(`${BACKEND_URL}/api/v1/internal/visit-summary${query}`, {
    method: "GET",
    cache: "no-store",
    headers: {
      "x-internal-admin-token": INTERNAL_ADMIN_TOKEN,
    },
  });

  const body = await upstream.text();
  return new Response(body, {
    status: upstream.status,
    headers: { "content-type": upstream.headers.get("content-type") || "application/json" },
  });
}
