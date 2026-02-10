import { kafka, GOOGLE_SEARCH_TOPIC, YOUTUBE_SEARCH_TOPIC, KAFKA_GROUP_ID } from "./lib.js";

interface VideoInfo {
  title: string;
  channelTitle: string;
  viewCount: number;
  videoId: string;
}

async function main() {
  const consumer = kafka.consumer({ groupId: `${KAFKA_GROUP_ID}-summary-${Date.now()}` });
  await consumer.connect();
  await consumer.subscribe({
    topics: [GOOGLE_SEARCH_TOPIC, YOUTUBE_SEARCH_TOPIC],
    fromBeginning: true,
  });

  const stats = {
    google: { total: 0, byCategory: {} as Record<string, number>, byKeyword: {} as Record<string, number> },
    youtube: {
      total: 0,
      byCategory: {} as Record<string, number>,
      byKeyword: {} as Record<string, number>,
      topVideos: [] as VideoInfo[],
    },
  };

  let resolve: () => void;
  const done = new Promise<void>((r) => { resolve = r; });
  const timeout = setTimeout(() => resolve(), 20000);

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      try {
        const val = JSON.parse(message.value?.toString() ?? "{}");
        const category = val.category || "unknown";
        const keyword = val.keyword || "unknown";

        if (topic === GOOGLE_SEARCH_TOPIC) {
          stats.google.total++;
          stats.google.byCategory[category] = (stats.google.byCategory[category] || 0) + 1;
          stats.google.byKeyword[keyword] = (stats.google.byKeyword[keyword] || 0) + 1;
        } else if (topic === YOUTUBE_SEARCH_TOPIC) {
          stats.youtube.total++;
          stats.youtube.byCategory[category] = (stats.youtube.byCategory[category] || 0) + 1;
          stats.youtube.byKeyword[keyword] = (stats.youtube.byKeyword[keyword] || 0) + 1;

          if (val.statistics?.viewCount && val.video) {
            stats.youtube.topVideos.push({
              title: val.video.title,
              channelTitle: val.video.channelTitle,
              viewCount: val.statistics.viewCount,
              videoId: val.video.videoId,
            });
          }
        }
      } catch {
        // skip
      }
    },
  });

  await done;
  clearTimeout(timeout);
  await consumer.disconnect();

  // ── 결과 출력 ──
  console.log(`\n${"=".repeat(60)}`);
  console.log("📊 Google + YouTube 데이터 취합 결과");
  console.log("=".repeat(60));

  // Google
  console.log(`\n🔍 Google 검색 결과: ${stats.google.total}건`);
  if (stats.google.total > 0) {
    console.log("── 카테고리별 ──");
    for (const [cat, cnt] of Object.entries(stats.google.byCategory).sort((a, b) => b[1] - a[1])) {
      console.log(`  ${cat}: ${cnt}건`);
    }
    console.log("── 키워드별 ──");
    for (const [kw, cnt] of Object.entries(stats.google.byKeyword).sort((a, b) => b[1] - a[1])) {
      console.log(`  ${kw}: ${cnt}건`);
    }
  }

  // YouTube
  console.log(`\n🎬 YouTube 검색 결과: ${stats.youtube.total}건`);
  if (stats.youtube.total > 0) {
    console.log("── 카테고리별 ──");
    for (const [cat, cnt] of Object.entries(stats.youtube.byCategory).sort((a, b) => b[1] - a[1])) {
      console.log(`  ${cat}: ${cnt}건`);
    }
    console.log("── 키워드별 ──");
    for (const [kw, cnt] of Object.entries(stats.youtube.byKeyword).sort((a, b) => b[1] - a[1])) {
      console.log(`  ${kw}: ${cnt}건`);
    }

    // 조회수 상위 10개 영상
    const topVideos = stats.youtube.topVideos.sort((a, b) => b.viewCount - a.viewCount).slice(0, 10);
    if (topVideos.length > 0) {
      console.log("\n── 조회수 TOP 10 영상 ──");
      topVideos.forEach((v, i) => {
        console.log(
          `  ${i + 1}. [${v.viewCount.toLocaleString()}회] ${v.title.substring(0, 50)} (${v.channelTitle})`,
        );
      });
    }
  }

  console.log("\n" + "=".repeat(60));
}

main().catch((e) => {
  console.error("오류:", (e as Error).message);
  process.exit(1);
});
