// index.js  — Node 18+（fetch が標準で使えます）

const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID;
const POSTS_TABLE = process.env.AIRTABLE_TABLE_POSTS || "HashtagPosts";
const TAGS_TABLE = process.env.AIRTABLE_TABLE_TAGS || "Hashtags";

const IG_USER_ID = process.env.IG_USER_ID;
const IG_TOKEN = process.env.IG_TOKEN;

const AIRTABLE_API = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}`;
const IG_API = "https://graph.facebook.com/v23.0";

// 安全にクォート（Airtable の filterByFormula 用）
const q = (s) => `'${String(s).replace(/'/g, "\\'")}'`;

// ---- Airtable helpers ----
const ah = {
  async select(table, params = {}) {
    const u = new URL(`${AIRTABLE_API}/${encodeURIComponent(table)}`);
    Object.entries(params).forEach(([k, v]) => u.searchParams.append(k, v));
    const r = await fetch(u, {
      headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}` },
    });
    if (!r.ok) throw new Error(`Airtable select ${table}: ${r.status} ${await r.text()}`);
    return r.json();
  },
  async create(table, fields) {
    const r = await fetch(`${AIRTABLE_API}/${encodeURIComponent(table)}`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${AIRTABLE_TOKEN}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ records: [{ fields }], typecast: true }),
    });
    if (!r.ok) throw new Error(`Airtable create ${table}: ${r.status} ${await r.text()}`);
    return r.json();
  },
  async update(table, id, fields) {
    const r = await fetch(`${AIRTABLE_API}/${encodeURIComponent(table)}`, {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${AIRTABLE_TOKEN}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ records: [{ id, fields }], typecast: true }),
    });
    if (!r.ok) throw new Error(`Airtable update ${table}: ${r.status} ${await r.text()}`);
    return r.json();
  },
};

// Hashtags: Active=TRUE のタグ一覧（Tagname と recordId）
async function getActiveTags() {
  const res = await ah.select(TAGS_TABLE, {
    filterByFormula: "{Active}",
    fields: ["Tagname"],
    pageSize: 100,
  });
  return res.records
    .map((r) => ({ id: r.id, name: r.fields.Tagname }))
    .filter((t) => t.name);
}

// Tag のレコードIDを名前から取得（なければ作成）
async function getOrCreateTagRecord(tagname) {
  const res = await ah.select(TAGS_TABLE, {
    filterByFormula: `LOWER({Tagname}) = LOWER(${q(tagname)})`,
    maxRecords: 1,
  });
  if (res.records.length) return res.records[0].id;
  const created = await ah.create(TAGS_TABLE, { Tagname: tagname, Active: true });
  return created.records[0].id;
}

// ---- Instagram helpers ----
async function resolveHashtagId(tag) {
  const u = new URL(`${IG_API}/ig_hashtag_search`);
  u.searchParams.set("user_id", IG_USER_ID);
  u.searchParams.set("q", tag);
  u.searchParams.set("access_token", IG_TOKEN);
  const r = await fetch(u);
  if (!r.ok) throw new Error(`ig_hashtag_search: ${r.status} ${await r.text()}`);
  const j = await r.json();
  return j.data?.[0]?.id || null;
}

async function fetchTopMedia(hashtagId, limit = 20) {
  const u = new URL(`${IG_API}/${hashtagId}/top_media`);
  u.searchParams.set("user_id", IG_USER_ID);
  u.searchParams.set(
    "fields",
    [
      "id",
      "media_type",
      "media_url",
      "permalink",
      "caption",
      "like_count",
      "comments_count",
      "timestamp",
    ].join(",")
  );
  u.searchParams.set("access_token", IG_TOKEN);
  u.searchParams.set("limit", String(limit));

  const r = await fetch(u);
  if (!r.ok) throw new Error(`top_media: ${r.status} ${await r.text()}`);
  const j = await r.json();
  return j.data || [];
}

// HashtagPosts に MediaID で upsert。Hashtag(s) のリンクは追加式で維持。
async function upsertPost(media, tagRecordId) {
  // 既存レコードを検索
  const found = await ah.select(POSTS_TABLE, {
    filterByFormula: `{MediaID} = ${q(media.id)}`,
    maxRecords: 1,
  });

  const fields = {
    MediaID: media.id,
    MediaType: media.media_type,
    MediaURL: media.media_url,
    Permalink: media.permalink,
    Caption: media.caption || "",
    LikeCount: media.like_count ?? 0,
    CommentsCount: media.comments_count ?? 0,
    Timestamp: media.timestamp,
    // 好きな重みづけ。Rank/Sort が Formula の場合は無視されるだけなので安心。
    "Rank/Sort": (media.like_count ?? 0) * 100 + (media.comments_count ?? 0) * 10,
  };

  if (found.records.length) {
    const rec = found.records[0];
    const existingLinks = Array.isArray(rec.fields["Hashtag(s)"])
      ? rec.fields["Hashtag(s)"].map((x) => ({ id: x }))
      : [];
    // すでにリンク済みでなければ追加
    if (!existingLinks.find((x) => x.id === tagRecordId)) {
      fields["Hashtag(s)"] = [...existingLinks, { id: tagRecordId }];
    }
    await ah.update(POSTS_TABLE, rec.id, fields);
  } else {
    fields["Hashtag(s)"] = [{ id: tagRecordId }];
    await ah.create(POSTS_TABLE, fields);
  }
}

async function main() {
  if (!AIRTABLE_TOKEN || !IG_TOKEN) {
    throw new Error("Missing secrets. Set AIRTABLE_* and IG_* secrets.");
  }

  const tags = await getActiveTags();
  if (!tags.length) {
    console.log("No active tags in Hashtags table. Nothing to do.");
    return;
  }

  console.log("Active tags:", tags.map((t) => t.name).join(", "));

  // タグごとに処理
  for (const t of tags) {
    const hashtagId = await resolveHashtagId(t.name);
    if (!hashtagId) {
      console.warn(`Skip: hashtagId not found for "${t.name}"`);
      continue;
    }
    const medias = await fetchTopMedia(hashtagId, 20);
    console.log(`Tag "${t.name}" -> ${medias.length} posts`);

    // Hashtag レコードID（なければ作っておく）
    const tagRecordId = t.id || (await getOrCreateTagRecord(t.name));

    for (const m of medias) {
      try {
        await upsertPost(m, tagRecordId);
      } catch (e) {
        console.error(`Upsert failed ${m.id}:`, e.message);
      }
    }
    // 軽いレートリミット対策
    await new Promise((r) => setTimeout(r, 800));
  }

  console.log("Done.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});

