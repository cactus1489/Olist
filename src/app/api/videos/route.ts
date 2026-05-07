import { NextResponse } from 'next/server';
import pool from '@/lib/db';

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const calendarName = searchParams.get('calendar') || '기본 캘린더';

  try {
    const result = await pool.query(
      'SELECT * FROM vcalendar_videos WHERE calendar_name = $1 ORDER BY added_at ASC',
      [calendarName]
    );
    return NextResponse.json(result.rows);
  } catch (error) {
    console.error('API Error:', error);
    return NextResponse.json({ error: 'Database error' }, { status: 500 });
  }
}

export async function POST(request: Request) {
  try {
    const { video_url, video_date, calendar_name } = await request.json();
    
    // 1. 영상 ID 추출
    const regex = /(?:v=|\/|embed\/|shorts\/|youtu.be\/)([0-9A-Za-z_-]{11})/;
    const video_id = video_url.match(regex)?.[1];
    if (!video_id) return NextResponse.json({ error: 'Invalid URL' }, { status: 400 });

    // 2. 제목 가져오기 (oEmbed)
    let title = '제목 없음';
    try {
      const oembed = await fetch(`https://www.youtube.com/oembed?url=${video_url}&format=json`).then(r => r.json());
      title = oembed.title;
    } catch {}

    // 3. 길이 가져오기 (YouTube Data API v3)
    let duration = 0;
    try {
      const apiKey = process.env.YOUTUBE_API_KEY;
      const apiRes = await fetch(`https://www.googleapis.com/youtube/v3/videos?id=${video_id}&part=contentDetails&key=${apiKey}`).then(r => r.json());
      const isoDur = apiRes.items[0]?.contentDetails?.duration;
      if (isoDur) {
        const match = isoDur.match(/PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?/);
        duration = (parseInt(match[1] || '0') * 3600) + (parseInt(match[2] || '0') * 60) + parseInt(match[3] || '0');
      }
    } catch {}

    // 4. DB 저장
    const now = new Date().toISOString().replace('T', ' ').substring(0, 19);
    await pool.query(
      'INSERT INTO vcalendar_videos (video_date, video_id, video_url, added_at, calendar_name, video_title, duration) VALUES ($1, $2, $3, $4, $5, $6, $7)',
      [video_date, video_id, video_url, now, calendar_name || '기본 캘린더', title, duration]
    );

    return NextResponse.json({ success: true, title });
  } catch (error) {
    console.error('POST Error:', error);
    return NextResponse.json({ error: 'Failed to add video' }, { status: 500 });
  }
}
