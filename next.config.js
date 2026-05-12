/** @type {import('next').NextConfig} */
const apiOrigin =
  process.env.VALIDOR_API_ORIGIN?.replace(/\/$/, "") ||
  "http://127.0.0.1:5000";

const nextConfig = {
  async rewrites() {
    return [
      {
        source: "/v1/:path*",
        destination: `${apiOrigin}/v1/:path*`,
      },
    ];
  },
};

module.exports = nextConfig;
