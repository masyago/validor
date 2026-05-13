/** @type {import('next').NextConfig} */
const apiPort = process.env.API_PORT || "5000";
const apiOrigin =
  process.env.VALIDOR_API_ORIGIN?.replace(/\/$/, "") ||
  `http://127.0.0.1:${apiPort}`;

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
