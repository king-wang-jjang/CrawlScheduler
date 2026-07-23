// Usage: mongosh ... kingwangjjang --file rewrite_mongo_dcinside_paths.js
// The moved-map path is supplied through MIGRATION_MOVED_MAP inside the container.

const fs = require("fs");
const movedMapPath = process.env.MIGRATION_MOVED_MAP || "/tmp/dcinside-moved.tsv";
const lines = fs
  .readFileSync(movedMapPath, "utf8")
  .split(/\r?\n/)
  .filter(Boolean);

function replacePrefix(value, oldPrefix, newPrefix) {
  if (typeof value !== "string") return value;
  if (value === oldPrefix) return newPrefix;
  if (value.startsWith(`${oldPrefix}/`)) {
    return `${newPrefix}${value.slice(oldPrefix.length)}`;
  }
  return value;
}

let updated = 0;
let missing = 0;

for (const line of lines) {
  const [postNo, newPrefix] = line.split("\t", 2);
  const oldPrefix = `Dcinside/dcbest/${postNo}`;
  const documents = db.Realtime.find({
    site: "dcinside",
    category: "dcbest",
    no: Number(postNo),
  });

  let found = false;
  documents.forEach((document) => {
    found = true;
    const contents = (document.contents || []).map((block) => {
      if (!block || typeof block !== "object") return block;
      const rewritten = { ...block };
      if ("path" in rewritten) {
        rewritten.path = replacePrefix(rewritten.path, oldPrefix, newPrefix);
      }
      if ("media_path" in rewritten) {
        rewritten.media_path = replacePrefix(
          rewritten.media_path,
          oldPrefix,
          newPrefix,
        );
      }
      return rewritten;
    });
    const thumbnail = replacePrefix(document.thumbnail, oldPrefix, newPrefix);
    db.Realtime.updateOne(
      { _id: document._id },
      { $set: { contents, thumbnail } },
    );
    updated += 1;
  });
  if (!found) missing += 1;
}

printjson({ moved: lines.length, updated, missing });
