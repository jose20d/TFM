const BACKEND_URL = process.env.BACKEND_API_URL || "http://127.0.0.1:8001";

export async function GET(request, context) {
  const params = await context.params;
  const path = (params?.path || []).join("/");
  const query = request.nextUrl.search || "";
  const url = `${BACKEND_URL}/${path}${query}`;

  const upstream = await fetch(url, { cache: "no-store" });
  const contentType = upstream.headers.get("content-type") || "application/json";
  const body = await upstream.text();

  return new Response(body, {
    status: upstream.status,
    headers: { "content-type": contentType },
  });
}
