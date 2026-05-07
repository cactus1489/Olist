'use client';

import { useEffect, useState, useMemo } from 'react';
import { Video, VideosByDate } from '@/lib/types';
import { ChevronLeft, ChevronRight, Plus, MonitorPlay, BarChart3, X, Clock } from 'lucide-react';
import { 
  format, addMonths, subMonths, startOfMonth, endOfMonth, eachDayOfInterval, 
  startOfWeek, endOfWeek, isSameDay, isWithinInterval, parseISO
} from 'date-fns';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell } from 'recharts';
import { motion, AnimatePresence } from 'framer-motion';

export default function CalendarPage() {
  const [activeTab, setActiveTab] = useState<'calendar' | 'stats'>('calendar');
  const [currentMonth, setCurrentMonth] = useState(new Date());
  const [videos, setVideos] = useState<VideosByDate>({});
  const [rawVideos, setRawVideos] = useState<Video[]>([]);
  const [loading, setLoading] = useState(true);
  
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [newUrl, setNewUrl] = useState('');
  const [targetDate, setTargetDate] = useState(format(new Date(), 'yyyy-MM-dd'));
  const [isSubmitting, setIsSubmitting] = useState(false);

  const fetchVideos = async () => {
    try {
      const res = await fetch('/api/videos?calendar=기본 캘린더');
      const data: Video[] = await res.json();
      setRawVideos(data);
      
      const grouped = data.reduce((acc: VideosByDate, video) => {
        const date = video.video_date;
        if (!acc[date]) acc[date] = [];
        acc[date].push(video);
        return acc;
      }, {});
      
      setVideos(grouped);
    } catch (error) {
      console.error('Failed to fetch videos', error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchVideos();
  }, []);

  // [수정] 일요일 시작 주간 데이터 계산
  const { statsData, weeklyVideoCount, totalWeeklyMinutes } = useMemo(() => {
    const today = new Date();
    const weekStart = startOfWeek(today, { weekStartsOn: 0 }); // 일요일 시작
    const weekEnd = endOfWeek(today, { weekStartsOn: 0 }); // 토요일 종료
    
    const weekInterval = { start: weekStart, end: weekEnd };
    const currentWeekDays = eachDayOfInterval(weekInterval);

    let weekCount = 0;
    let weekMinutes = 0;

    const formattedData = currentWeekDays.map(date => {
      const dateStr = format(date, 'yyyy-MM-dd');
      const dayVideos = videos[dateStr] || [];
      const totalSeconds = dayVideos.reduce((sum, v) => sum + (v.duration || 0), 0);
      const mins = Math.round(totalSeconds / 60);
      
      weekCount += dayVideos.length;
      weekMinutes += mins;

      return {
        name: format(date, 'EEE').toUpperCase(),
        minutes: mins,
        date: dateStr
      };
    });

    return { 
      statsData: formattedData, 
      weeklyVideoCount: weekCount, 
      totalWeeklyMinutes: weekMinutes 
    };
  }, [videos]);

  const handleAddVideo = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newUrl) return;
    setIsSubmitting(true);
    try {
      const res = await fetch('/api/videos', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          video_url: newUrl, video_date: targetDate, calendar_name: '기본 캘린더'
        })
      });
      if (res.ok) {
        setNewUrl('');
        setIsModalOpen(false);
        fetchVideos();
      }
    } catch (error) {
      alert('오류 발생');
    } finally {
      setIsSubmitting(false);
    }
  };

  const days = eachDayOfInterval({
    start: startOfMonth(currentMonth),
    end: endOfMonth(currentMonth),
  });

  return (
    <div className="min-h-screen bg-[#0f172a] text-white font-sans pb-24 select-none">
      <header className="p-6 bg-[#1e293b]/50 backdrop-blur-md sticky top-0 z-40 border-b border-white/5">
        <div className="flex justify-between items-center mb-4">
          <div>
            <h1 className="text-2xl font-black tracking-tighter text-red-500">Y-CALENDAR</h1>
            <p className="text-[10px] text-slate-400 font-bold tracking-[0.2em]">{activeTab === 'calendar' ? format(currentMonth, 'MMMM yyyy').toUpperCase() : 'WEEKLY DASHBOARD'}</p>
          </div>
          {activeTab === 'calendar' && (
            <div className="flex gap-1 bg-black/20 p-1 rounded-full">
              <button onClick={() => setCurrentMonth(subMonths(currentMonth, 1))} className="p-2 hover:bg-white/10 rounded-full transition-all active:scale-90"><ChevronLeft size={20} /></button>
              <button onClick={() => setCurrentMonth(addMonths(currentMonth, 1))} className="p-2 hover:bg-white/10 rounded-full transition-all active:scale-90"><ChevronRight size={20} /></button>
            </div>
          )}
        </div>
      </header>

      <main className="p-4 overflow-x-hidden">
        <AnimatePresence mode="wait">
          {activeTab === 'calendar' ? (
            <motion.div key="calendar" initial={{ opacity: 0, x: -20 }} animate={{ opacity: 1, x: 0 }} exit={{ opacity: 0, x: 20 }}>
              <div className="grid grid-cols-7 gap-1 mb-2">
                {['SUN', 'MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT'].map(d => (
                  <div key={d} className="text-[9px] font-black text-slate-600 text-center py-2">{d}</div>
                ))}
              </div>
              <div className="grid grid-cols-7 gap-2">
                {days.map((day) => {
                  const dateStr = format(day, 'yyyy-MM-dd');
                  const dayVideos = videos[dateStr] || [];
                  const hasVideo = dayVideos.length > 0;
                  const totalMin = Math.round(dayVideos.reduce((s, v) => s + (v.duration || 0), 0) / 60);
                  return (
                    <div key={dateStr} className={`aspect-square relative rounded-xl overflow-hidden border transition-all active:scale-95 group ${hasVideo ? 'border-red-500/50 bg-black' : 'border-white/5 bg-[#1e293b]/30'}`}>
                      <span className={`absolute top-1 left-1.5 text-[9px] font-black z-20 ${hasVideo ? 'text-white' : 'text-slate-500'}`}>{format(day, 'd')}</span>
                      {hasVideo ? (
                        <a href={dayVideos[dayVideos.length-1].video_url} target="_blank" rel="noopener noreferrer" className="block w-full h-full">
                          <img src={`https://img.youtube.com/vi/${dayVideos[dayVideos.length-1].video_id}/mqdefault.jpg`} className="w-full h-full object-cover opacity-60 group-active:opacity-100" alt="thumb" />
                          <div className="absolute top-1 right-1 flex flex-col items-end gap-1 z-20">
                            {dayVideos.length > 1 && <span className="bg-red-600 text-[6px] font-black px-1 rounded">+{dayVideos.length - 1}</span>}
                            <span className="bg-black/60 backdrop-blur-sm text-[6px] font-black px-1 py-0.5 rounded text-white flex items-center gap-0.5 shadow-md"><Clock size={5} />{totalMin}m</span>
                          </div>
                        </a>
                      ) : (
                        <button onClick={() => { setTargetDate(dateStr); setIsModalOpen(true); }} className="w-full h-full opacity-0 hover:opacity-100 flex items-center justify-center transition-opacity"><Plus size={14} className="text-slate-600" /></button>
                      )}
                    </div>
                  );
                })}
              </div>
            </motion.div>
          ) : (
            <motion.div key="stats" initial={{ opacity: 0, x: 20 }} animate={{ opacity: 1, x: 0 }} exit={{ opacity: 0, x: -20 }} className="space-y-6">
              <div className="bg-[#1e293b] rounded-3xl p-6 border border-white/5 shadow-xl">
                <p className="text-xs font-black text-slate-400 mb-1 uppercase tracking-tighter">Current Week Effort</p>
                <div className="flex items-baseline gap-2">
                  <h2 className="text-4xl font-black text-red-500">{totalWeeklyMinutes}</h2>
                  <span className="text-lg font-bold text-slate-300 italic">MINUTES</span>
                </div>
              </div>

              <div className="bg-[#1e293b] rounded-3xl p-6 border border-white/5 shadow-xl h-80">
                <h3 className="text-xs font-black text-slate-400 mb-6 uppercase tracking-widest text-center">Exercise Time (SUN - SAT)</h3>
                <ResponsiveContainer width="100%" height="80%">
                  <BarChart data={statsData}>
                    <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="rgba(255,255,255,0.05)" />
                    <XAxis dataKey="name" axisLine={false} tickLine={false} tick={{fill: '#475569', fontSize: 10, fontWeight: 800}} dy={10} />
                    <YAxis hide />
                    <Tooltip 
                      cursor={{fill: 'rgba(255,255,255,0.05)'}}
                      contentStyle={{backgroundColor: '#0f172a', borderRadius: '12px', border: 'none', color: '#fff'}}
                      itemStyle={{color: '#ef4444', fontWeight: '900'}}
                    />
                    <Bar dataKey="minutes" radius={[6, 6, 0, 0]}>
                      {statsData.map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={isSameDay(parseISO(entry.date), new Date()) ? '#ffffff' : (entry.minutes > 0 ? '#ef4444' : '#334155')} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div className="bg-black/20 p-5 rounded-2xl border border-white/5">
                  <p className="text-[10px] font-black text-slate-500 uppercase mb-1 tracking-wider">Weekly Videos</p>
                  <p className="text-2xl font-black">{weeklyVideoCount}</p>
                </div>
                <div className="bg-black/20 p-5 rounded-2xl border border-white/5">
                  <p className="text-[10px] font-black text-slate-500 uppercase mb-1 tracking-wider">Weekly Avg</p>
                  <p className="text-2xl font-black">{Math.round(totalWeeklyMinutes / 7)}m</p>
                </div>
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </main>

      <AnimatePresence>
        {isModalOpen && (
          <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} className="fixed inset-0 bg-black/80 backdrop-blur-md z-[100] flex items-end sm:items-center justify-center p-4">
            <motion.div initial={{ y: 300 }} animate={{ y: 0 }} exit={{ y: 300 }} className="bg-[#1e293b] w-full max-w-md rounded-t-3xl sm:rounded-3xl p-8 border-t sm:border border-white/10 shadow-2xl">
              <div className="flex justify-between items-center mb-6">
                <h2 className="text-xl font-black italic text-white flex items-center gap-2"><Plus className="text-red-500" /> ADD RECORD</h2>
                <button onClick={() => setIsModalOpen(false)} className="text-slate-400 hover:text-white transition-colors"><X size={24} /></button>
              </div>
              <form onSubmit={handleAddVideo} className="space-y-6">
                <div>
                  <label className="text-[10px] font-black text-slate-500 uppercase tracking-widest mb-2 block">Date</label>
                  <input type="date" value={targetDate} onChange={(e) => setTargetDate(e.target.value)} className="w-full bg-[#0f172a] border border-white/5 rounded-xl px-4 py-3 text-white outline-none focus:border-red-500/50 transition-all font-bold" />
                </div>
                <div>
                  <label className="text-[10px] font-black text-slate-500 uppercase tracking-widest mb-2 block">Youtube URL</label>
                  <input type="url" placeholder="Paste link here..." value={newUrl} onChange={(e) => setNewUrl(e.target.value)} className="w-full bg-[#0f172a] border border-white/5 rounded-xl px-4 py-3 text-white outline-none focus:border-red-500/50 transition-all font-bold" required />
                </div>
                <button type="submit" disabled={isSubmitting} className="w-full bg-red-600 hover:bg-red-500 disabled:bg-slate-700 text-white font-black py-4 rounded-2xl shadow-xl shadow-red-600/20 active:scale-95 transition-all text-xs tracking-[0.2em] font-bold italic">
                  {isSubmitting ? 'PROCESSING...' : 'CONFIRM RECORD'}
                </button>
              </form>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>

      <nav className="fixed bottom-0 left-0 right-0 bg-[#1e293b]/90 backdrop-blur-xl border-t border-white/5 px-8 pt-4 pb-8 flex justify-between items-center z-40">
        <button onClick={() => setActiveTab('calendar')} className={`flex flex-col items-center gap-1 transition-colors ${activeTab === 'calendar' ? 'text-red-500' : 'text-slate-500'}`}>
          <MonitorPlay size={22} strokeWidth={activeTab === 'calendar' ? 2.5 : 2} />
          <span className="text-[9px] font-black tracking-tighter">CALENDAR</span>
        </button>
        <button onClick={() => { setTargetDate(format(new Date(), 'yyyy-MM-dd')); setIsModalOpen(true); }} className="bg-red-600 p-4 rounded-2xl shadow-lg shadow-red-600/30 -mt-16 border-4 border-[#0f172a] active:scale-95 transition-all hover:bg-red-500 group">
          <Plus size={26} color="white" strokeWidth={3} className="group-hover:rotate-90 transition-transform duration-300" />
        </button>
        <button onClick={() => setActiveTab('stats')} className={`flex flex-col items-center gap-1 transition-colors ${activeTab === 'stats' ? 'text-red-500' : 'text-slate-500'}`}>
          <BarChart3 size={22} strokeWidth={activeTab === 'stats' ? 2.5 : 2} />
          <span className="text-[9px] font-black tracking-tighter">STATISTICS</span>
        </button>
      </nav>

      {loading && (
        <div className="fixed inset-0 bg-[#0f172a] flex items-center justify-center z-[200]">
          <div className="w-10 h-10 border-4 border-red-500 border-t-transparent rounded-full animate-spin shadow-lg"></div>
        </div>
      )}
    </div>
  );
}
