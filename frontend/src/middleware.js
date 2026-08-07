import { NextResponse } from "next/server";

const ADMIN_USER = process.env.ADMIN_PANEL_USER || "";
const ADMIN_PASSWORD = process.env.ADMIN_PANEL_PASSWORD || "";

function unauthorized() {
  return new NextResponse("Authentication required.", {
    status: 401,
    headers: {
      "WWW-Authenticate": 'Basic realm="GeoContext Internal Panel"',
      "X-Robots-Tag": "noindex, nofollow",
    },
  });
}

export function middleware(request) {
  const authHeader = request.headers.get("authorization") || "";
  if (!ADMIN_USER || !ADMIN_PASSWORD) {
    return new NextResponse("Admin credentials are not configured.", { status: 503 });
  }

  if (!authHeader.startsWith("Basic ")) {
    return unauthorized();
  }

  const encoded = authHeader.slice("Basic ".length).trim();
  const decoded = atob(encoded);
  const separator = decoded.indexOf(":");
  const user = separator >= 0 ? decoded.slice(0, separator) : "";
  const password = separator >= 0 ? decoded.slice(separator + 1) : "";

  if (user !== ADMIN_USER || password !== ADMIN_PASSWORD) {
    return unauthorized();
  }

  const response = NextResponse.next();
  response.headers.set("X-Robots-Tag", "noindex, nofollow");
  return response;
}

export const config = {
  matcher: ["/ctr-geo/:path*", "/api/internal-admin/:path*", "/internal-admin/:path*"],
};
