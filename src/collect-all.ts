import { execSync } from "child_process";
import path from "path";

console.log("🚀 Google + YouTube 전체 수집 시작\n");

const projectRoot = path.resolve(__dirname, "..");

try {
  console.log("━".repeat(60));
  console.log("📌 [1/2] Google Custom Search 수집");
  console.log("━".repeat(60));
  execSync("npx tsx src/collect-google.ts", { stdio: "inherit", cwd: projectRoot });

  console.log("\n" + "━".repeat(60));
  console.log("📌 [2/2] YouTube Data API 수집");
  console.log("━".repeat(60));
  execSync("npx tsx src/collect-youtube.ts", { stdio: "inherit", cwd: projectRoot });

  console.log("\n\n✅ 전체 수집 완료! 'npm run summary' 로 결과를 확인하세요.");
} catch (e) {
  console.error("수집 중 오류 발생:", (e as Error).message);
  process.exit(1);
}
