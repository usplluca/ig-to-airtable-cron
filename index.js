import fetch from "node-fetch";
import Airtable from "airtable";

const {
  AIRTABLE_TOKEN,
  AIRTABLE_BASE_ID,
  AIRTABLE_TABLE_POSTS, // HashtagPosts
  AIRTABLE_TABLE_TAGS,  // Hashtags
  IG_USER_ID,           // 1784... のInstagram User ID
  IG_TOKEN              // 60日トークン（システムユーザーで発行）
} = process.env;

// Airtable client
const base = new Airtable({ apiKey: AIRTABLE_TOKEN }).base(AIRTABLE_BASE_ID);

// 小休止
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// IG API 呼び出し
async function igFetch(path, params = {}) {
  const url = new URL(`https://graph.facebook.com/v23.0/${path}`);
  url.searchParams.set("access_token", IG_TOKEN);
  for (const [k, v] of Object.entries(params)) url.searchParams.set(k, v);
  const res = await fetch(url);
  const json = await res.json();
  if (!res.ok) throw new Error(`IG API ${res.status}: ${JSON.stringify(json)}`);
  return json;
}

// HashtagPosts から MediaID 重複チェック
async function findPostByMediaId(mediaId) {
  const formula = `{MediaID}='${String(mediaId).replace(/'/g, "\\'")}'`;
  const recs = await base(AIRTABLE_TABLE_POSTS).select({
    filterByFormula: formula,
    maxRecords: 1
  }).all();
  return recs[0];
}

// Active=1 のハッシュタグ（レコードIDつき）
async function getActiveTags() {
  const recs = await base(AIRTABLE_TABLE_TAGS).select({
    fields: ["Tagname", "Active"],
    filterByFormula: "{Active}=1"
  }).all();
  const map = new Map();
  for (const r of recs) {
    const tag = r.get("Tagname");
    if (tag) map.set(String(tag).trim(), { recId: r.id, tag });
  }
  return map;
}

async function run() {
  console.log("Start: IG -> Airtable");

  const tags = await getActiveTags();
  if (tags.size === 0) {
    console.log("Active なタグが無いので終了");
    return;
  }

  for (const [tagName, { recId: tagRecId }] of tags.entries()) {
    try {
      // hashtag_id を取得
      const search = await igFetch("ig_hashtag_search", {
        user_id: IG_USER_ID,
        q: tagName
      });
      const hashtagId = search?.data?.[0]?.id;
      if (!hashtagId) {
        console.log(`Hashtag not found: ${tagName}`);
        continue;
      }

      // recent_media を取得（必要なら top_media に変更可）
      const media = await igFetch(`${hashtagId}/recent_media`, {
        user_id: IG_USER_ID,
        fields: [
          "id",
          "media_type",
          "media_url",
          "permalink",
          "caption",
          "like_count",
          "comments_count",
          "timestamp"
        ].join(",")
      });

      const items = media?.data ?? [];
      console.log(`Tag ${tagName}: ${items.length} posts`);

      for (const m of items) {
      const fields = {
  MediaID: m.id,
  Hashtag: [{ id: tagRecId }],              // ← リンク先(Hashtags)のレコードIDだけ渡す
  MediaType: m.media_type || null,
  MediaURL: m.media_url || null,
  Permalink: m.permalink || null,
  Caption: m.caption ?? "",
  LikeCount: m.like_count ?? 0,
  CommentsCount: m.comments_count ?? 0,
  Timestamp: new Date((m.timestamp ?? Math.floor(Date.now()/1000)) * 1000).toISOString()
};


        const exist = await findPostByMediaId(m.id);
        if (exist) {
          await base(AIRTABLE_TABLE_POSTS).update(exist.id, fields);
          console.log(`updated: ${m.id} (${tagName})`);
        } else {
          await base(AIRTABLE_TABLE_POSTS).create(fields);
          console.log(`created: ${m.id} (${tagName})`);
        }
        await sleep(200);
      }
    } catch (e) {
      console.error(`Tag ${tagName} failed: ${e.message}`);
      // 次のタグへ
    }
  }

  console.log("Done.");
}

run().catch(e => {
  console.error(e);
  process.exit(1);
});
