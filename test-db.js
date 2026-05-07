const { Pool } = require('pg');

const pool = new Pool({
  user: 'postgres.goxqbrcbkkbhdgdmnwbk',
  host: 'aws-1-ap-northeast-2.pooler.supabase.com',
  database: 'postgres',
  password: 'Cactus!Q@W#E$R',
  port: 5432,
  ssl: { rejectUnauthorized: false }
});

console.log('Retrying with original Supabase pooler host...');

pool.query('SELECT current_database(), now()', (err, res) => {
  if (err) {
    console.error('❌ Connection Failed:', err.message);
  } else {
    console.log('✅ Connection Success!');
    console.log('Result:', res.rows[0]);
  }
  pool.end();
});
