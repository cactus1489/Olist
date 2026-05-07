export interface Video {
  id: number;
  video_date: string;
  video_id: string;
  video_url: string;
  added_at: string;
  calendar_name: string;
  video_title: string;
  duration: number;
}

export type VideosByDate = Record<string, Video[]>;

export interface CalendarInfo {
  id: number;
  calendar_name: string;
}
