import { Pool } from 'pg';

declare global {
  var _pgPool: Pool | undefined;
}

const pool = global._pgPool || new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false,
  },
  keepAlive: true,
});

// 연결 테스트용 로그
pool.on('error', (err) => {
  console.error('Unexpected error on idle client', err);
});

if (process.env.NODE_ENV !== 'production') {
  global._pgPool = pool;
}

export default pool;
