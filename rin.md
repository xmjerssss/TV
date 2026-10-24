及的 3 个文件的完整代码。

---

### 一、`server/src/services/backup.ts`（后端完整代码）

在服务端中增加：
1. `GET /backup/github-config`：获取当前 GitHub 仓库与 Token 绑定状态（Token 脱敏）。
2. `POST /backup/github-config`：保存/更新 GitHub 仓库配置（Token 经过与存储密钥相同的 AES-GCM 工业级加密后入库，绝对安全）。
3. `POST /backup/github-test`：测试连接，并自动检测目标仓库是否为私有仓库（Private）。
4. `POST /backup/export-to-github`：将全量数据打包为一个标准快照，通过 GitHub API 自动提交（Commit）到目标仓库的 `backups/` 目录。
5. `GET /backup/github-list`：直接通过 GitHub API 列出仓库下所有的 `rin-backup-*.json` 文件，**完全不需要任何 index 文件，永远不会出现列表为空的情况**。
6. `POST /backup/restore-from-github`：从 GitHub 下载指定的备份文件并在 D1 数据库中安全执行还原。

```typescript
import { inArray } from "drizzle-orm";
import { Hono } from "hono";
import type { AppContext } from "../core/hono-types";
import { adminOnly } from "../core/route-boundaries";
import {
    cache,
    comments,
    feedHashtags,
    feeds,
    friends,
    hashtags,
    moments,
    storageFiles,
    storages,
    users,
    visitStats,
} from "../db/schema";
import { createEnvDriverIfConfigured } from "../utils/storage-drivers";
import { driverForRow, DEFAULT_STORAGE_ID } from "../utils/storage-manager";
import { encryptSecret, decryptSecret } from "../utils/storage-secrets";

const BACKUP_SCHEMA_VERSION = 1;
const MAX_CLOUD_BACKUPS = 3;
const BACKUP_DIR_NAME = "backups";
const CLOUD_BACKUPS_CACHE_KEY = "cloud_backups_list";
const GITHUB_BACKUP_CACHE_KEY = "github_backup_config";

interface StoredGitHubConfig {
    owner: string;
    repo: string;
    branch: string;
    tokenEnc: string;
    path: string;
}

/** 获取并解密 GitHub 备份配置 */
async function getGitHubBackupConfig(env: Env, cacheManager: any): Promise<{
    owner: string;
    repo: string;
    branch: string;
    token: string;
    path: string;
    configured: boolean;
}> {
    const raw = (await cacheManager.get(GITHUB_BACKUP_CACHE_KEY)) as StoredGitHubConfig | null;
    if (!raw || !raw.owner || !raw.repo || !raw.tokenEnc) {
        return { owner: "", repo: "", branch: "main", token: "", path: "backups", configured: false };
    }
    try {
        const token = await decryptSecret(env, raw.tokenEnc);
        return {
            owner: raw.owner.trim(),
            repo: raw.repo.trim(),
            branch: (raw.branch || "main").trim(),
            token,
            path: (raw.path || "backups").trim().replace(/^\/+|\/+$/g, ""),
            configured: Boolean(raw.owner && raw.repo && token),
        };
    } catch {
        return { owner: raw.owner, repo: raw.repo, branch: raw.branch || "main", token: "", path: raw.path || "backups", configured: false };
    }
}

/** 获取当前系统可用的最佳存储驱动并规划备份目录 */
async function getActiveStorageDriver(env: Env, db: any) {
    const customStorages = await db.select().from(storages);
    const active = customStorages.find((s: any) => s.enabled === 1 && s.status === "ok");
    if (active) {
        const driver = await driverForRow(env, active);
        const folder = active.folder ? `${active.folder}/${BACKUP_DIR_NAME}` : BACKUP_DIR_NAME;
        return { driver, folder, name: active.name, bucket: active.bucket, id: active.id };
    }
    const envDriver = createEnvDriverIfConfigured(env);
    if (envDriver) {
        const cleanEnvFolder = envDriver.folder.replace(/images\/?$/, "");
        const folder = cleanEnvFolder ? `${cleanEnvFolder}/${BACKUP_DIR_NAME}` : BACKUP_DIR_NAME;
        return { driver: envDriver.driver, folder, name: "环境变量默认存储", bucket: env.S3_BUCKET || "default", id: DEFAULT_STORAGE_ID };
    }
    return null;
}

/** 智能净化用户输入的云端文件路径 */
function cleanBackupObjectKey(rawKey: string, bucketName?: string, storageName?: string): string {
    let key = rawKey.trim().replace(/^\/+/, "");
    if (storageName && key.startsWith(`${storageName}/`)) {
        key = key.slice(storageName.length + 1).replace(/^\/+/, "");
    }
    if (bucketName && key.startsWith(`${bucketName}/`)) {
        key = key.slice(bucketName.length + 1).replace(/^\/+/, "");
    }
    return key;
}

export function BackupService(): Hono {
    const app = new Hono();

    // 构建完整数据快照
    async function createSnapshot(db: any) {
        const [
            userRows,
            feedRows,
            momentRows,
            friendRows,
            commentRows,
            hashtagRows,
            feedHashtagRows,
            configRows,
            storageRows,
            storageFileRows,
            visitStatRows,
        ] = await Promise.all([
            db.select().from(users),
            db.select().from(feeds),
            db.select().from(moments),
            db.select().from(friends),
            db.select().from(comments),
            db.select().from(hashtags),
            db.select().from(feedHashtagRows || feedHashtags),
            db.select().from(cache).where(inArray(cache.type, ["client.config", "server.config"])),
            db.select().from(storages),
            db.select().from(storageFiles),
            db.select().from(visitStats),
        ]);

        const totalMediaBytes = storageFileRows.reduce((acc: number, f: any) => acc + (f.size || 0), 0);

        return {
            version: BACKUP_SCHEMA_VERSION,
            app: "rin",
            exportedAt: new Date().toISOString(),
            metadata: {
                counts: {
                    feeds: feedRows.length,
                    moments: momentRows.length,
                    comments: commentRows.length,
                    friends: friendRows.length,
                    hashtags: hashtagRows.length,
                    storages: storageRows.length,
                    mediaFiles: storageFileRows.length,
                },
                mediaSummary: {
                    totalFiles: storageFileRows.length,
                    totalBytes: totalMediaBytes,
                },
            },
            data: {
                users: userRows,
                feeds: feedRows,
                moments: momentRows,
                friends: friendRows,
                comments: commentRows,
                hashtags: hashtagRows,
                feedHashtags: feedHashtagRows,
                configs: configRows,
                storages: storageRows,
                storageFiles: storageFileRows,
                visitStats: visitStatRows,
            },
        };
    }

    // 执行数据恢复核心逻辑（严格防 SQL 变量溢出）
    async function executeRestore(db: any, cacheManager: any, payload: any) {
        if (!payload || !payload.data || payload.app !== "rin") {
            throw new Error("无效的备份文件结构，请确保使用的是 Rin 导出的备份包");
        }

        const data = payload.data;

        // 动态计算列数，确保单次 SQL 变量数保持在 30 个以内
        async function safeInsert<T extends Record<string, any>>(table: any, rows: T[]) {
            if (!rows || rows.length === 0) return;
            const colCount = Math.max(1, Object.keys(rows[0]).length);
            const chunkSize = Math.max(1, Math.floor(30 / colCount));
            for (let i = 0; i < rows.length; i += chunkSize) {
                const chunk = rows.slice(i, i + chunkSize);
                await db.insert(table).values(chunk as any);
            }
        }

        const parseDate = (d: any) => (d ? new Date(d) : new Date());

        // 1. 清理原有数据（按外键依赖逆序清理）
        await db.delete(feedHashtags);
        await db.delete(comments);
        await db.delete(visitStats);
        await db.delete(feeds);
        await db.delete(hashtags);
        await db.delete(moments);
        await db.delete(friends);
        await db.delete(storageFiles);
        await db.delete(storages);
        await db.delete(cache).where(inArray(cache.type, ["client.config", "server.config"]));
        await db.delete(users);

        // 2. 依次按依赖关系安全恢复
        if (data.users?.length) {
            await safeInsert(users, data.users.map((u: any) => ({
                ...u,
                createdAt: parseDate(u.createdAt),
                updatedAt: parseDate(u.updatedAt),
            })));
        }

        if (data.hashtags?.length) {
            await safeInsert(hashtags, data.hashtags.map((h: any) => ({
                ...h,
                createdAt: parseDate(h.createdAt),
                updatedAt: parseDate(h.updatedAt),
            })));
        }

        if (data.feeds?.length) {
            await safeInsert(feeds, data.feeds.map((f: any) => ({
                ...f,
                createdAt: parseDate(f.createdAt),
                updatedAt: parseDate(f.updatedAt),
            })));
        }

        if (data.feedHashtags?.length) {
            await safeInsert(feedHashtags, data.feedHashtags.map((fh: any) => ({
                ...fh,
                createdAt: parseDate(fh.createdAt),
                updatedAt: parseDate(fh.updatedAt),
            })));
        }

        if (data.comments?.length) {
            await safeInsert(comments, data.comments.map((cm: any) => ({
                ...cm,
                createdAt: parseDate(cm.createdAt),
                updatedAt: parseDate(cm.updatedAt),
            })));
        }

        if (data.moments?.length) {
            await safeInsert(moments, data.moments.map((m: any) => ({
                ...m,
                createdAt: parseDate(m.createdAt),
                updatedAt: parseDate(m.updatedAt),
            })));
        }

        if (data.friends?.length) {
            await safeInsert(friends, data.friends.map((fr: any) => ({
                ...fr,
                createdAt: parseDate(fr.createdAt),
                updatedAt: parseDate(fr.updatedAt),
            })));
        }

        if (data.storages?.length) {
            await safeInsert(storages, data.storages.map((s: any) => ({
                ...s,
                createdAt: parseDate(s.createdAt),
                updatedAt: parseDate(s.updatedAt),
            })));
        }

        if (data.storageFiles?.length) {
            await safeInsert(storageFiles, data.storageFiles.map((sf: any) => ({
                ...sf,
                createdAt: parseDate(sf.createdAt),
            })));
        }

        if (data.configs?.length) {
            await safeInsert(cache, data.configs.map((cf: any) => ({
                ...cf,
                createdAt: parseDate(cf.createdAt),
                updatedAt: parseDate(cf.updatedAt),
            })));
        }

        if (data.visitStats?.length) {
            await safeInsert(visitStats, data.visitStats.map((vs: any) => ({
                ...vs,
                updatedAt: parseDate(vs.updatedAt),
            })));
        }

        // 清空缓存
        await cacheManager.clear();
    }

    // 0. GET /backup/preview —— 获取即将备份的数据量预览
    app.get("/preview", adminOnly(async (c: AppContext) => {
        const db = c.get("db");
        const env = c.get("env");
        const cacheManager = c.get("cache");

        const [feedCount, commentCount, momentCount, friendCount, storageCount] = await Promise.all([
            db.select().from(feeds),
            db.select().from(comments),
            db.select().from(moments),
            db.select().from(friends),
            db.select().from(storages),
        ]);

        const target = await getActiveStorageDriver(env, db);
        const gh = await getGitHubBackupConfig(env, cacheManager);

        return c.json({
            ok: true,
            preview: {
                feedsCount: feedCount.length,
                commentsCount: commentCount.length,
                momentsCount: momentCount.length,
                friendsCount: friendCount.length,
                storagesCount: storageCount.length,
            },
            targetStorage: {
                configured: Boolean(target),
                name: target?.name || "未配置",
                folder: target ? target.folder : "",
            },
            targetGitHub: {
                configured: gh.configured,
                repo: gh.configured ? `${gh.owner}/${gh.repo}` : "未配置",
                branch: gh.branch,
                path: gh.path,
            },
        });
    }));

    // 1. GET /backup/export —— 下载本地备份 JSON 文件
    app.get("/export", adminOnly(async (c: AppContext) => {
        const db = c.get("db");
        const snapshot = await createSnapshot(db);
        const jsonStr = JSON.stringify(snapshot, null, 2);
        const dateStr = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
        const fileName = `rin-backup-${dateStr}.json`;

        return new Response(jsonStr, {
            status: 200,
            headers: {
                "Content-Type": "application/json; charset=utf-8",
                "Content-Disposition": `attachment; filename="${fileName}"`,
                "Cache-Control": "no-store",
            },
        });
    }));

    // 2. POST /backup/export-to-storage —— 备份并存入云存储专属 backups/ 目录
    app.post("/export-to-storage", adminOnly(async (c: AppContext) => {
        const db = c.get("db");
        const env = c.get("env");
        const cacheManager = c.get("cache");

        const target = await getActiveStorageDriver(env, db);
        if (!target) {
            return c.json({ ok: false, error: "未找到任何可用的存储桶，请先在「存储管理」绑定一个存储。" }, 400);
        }

        const snapshot = await createSnapshot(db);
        const jsonStr = JSON.stringify(snapshot, null, 2);
        const dateStr = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
        const fileName = `rin-backup-${dateStr}.json`;
        const objectKey = `${target.folder}/${fileName}`.replace(/\/+/g, "/");
        const indexKey = `${target.folder}/index.json`.replace(/\/+/g, "/");

        try {
            await target.driver.put(objectKey, jsonStr, "application/json");

            let backupList: any[] = [];
            try {
                const indexRes = await target.driver.get(indexKey);
                if (indexRes && indexRes.ok) {
                    const text = await indexRes.text();
                    backupList = JSON.parse(text);
                    if (!Array.isArray(backupList)) backupList = [];
                }
            } catch {
                backupList = [];
            }

            const newEntry = {
                key: objectKey,
                filename: fileName,
                createdAt: snapshot.exportedAt,
                metadata: snapshot.metadata,
            };

            backupList = backupList.filter((b) => b.key !== objectKey);
            backupList.unshift(newEntry);

            let prunedCount = 0;
            if (backupList.length > MAX_CLOUD_BACKUPS) {
                const toPrune = backupList.slice(MAX_CLOUD_BACKUPS);
                backupList = backupList.slice(0, MAX_CLOUD_BACKUPS);
                for (const old of toPrune) {
                    if (old.key) {
                        await target.driver.delete(old.key).catch(() => {});
                        prunedCount++;
                    }
                }
            }

            await target.driver.put(indexKey, JSON.stringify(backupList, null, 2), "application/json");
            await cacheManager.set(CLOUD_BACKUPS_CACHE_KEY, backupList).catch(() => {});

            return c.json({
                ok: true,
                filename: fileName,
                target: target.name,
                path: objectKey,
                exportedAt: snapshot.exportedAt,
                metadata: snapshot.metadata,
                totalBackups: backupList.length,
                prunedCount,
            });
        } catch (error: any) {
            return c.json({ ok: false, error: `写入存储桶失败: ${error.message || String(error)}` }, 500);
        }
    }));

    // 3. GET /backup/storage-list —— 获取存储桶中的备份列表
    app.get("/storage-list", adminOnly(async (c: AppContext) => {
        const db = c.get("db");
        const env = c.get("env");
        const cacheManager = c.get("cache");

        const target = await getActiveStorageDriver(env, db);
        if (!target) {
            return c.json({ ok: false, error: "未配置存储桶" }, 400);
        }

        const indexKey = `${target.folder}/index.json`.replace(/\/+/g, "/");
        let items: any[] = [];
        let readError: string | null = null;
        try {
            const indexRes = await target.driver.get(indexKey);
            if (indexRes && indexRes.ok) {
                const text = await indexRes.text();
                items = JSON.parse(text);
                if (!Array.isArray(items)) items = [];
            }
        } catch (err: any) {
            readError = err?.message || String(err);
            console.error("[backup] storage-list read error:", err);
        }

        if (items.length === 0) {
            try {
                const cached = await cacheManager.get(CLOUD_BACKUPS_CACHE_KEY);
                if (Array.isArray(cached) && cached.length > 0) {
                    items = cached;
                }
            } catch {}
        }

        return c.json({
            ok: true,
            target: target.name,
            items,
            error: items.length === 0 ? readError : undefined,
        });
    }));

    // 4. POST /backup/inspect-storage-backup —— 检查并预览存储桶备份文件
    app.post("/inspect-storage-backup", adminOnly(async (c: AppContext) => {
        const db = c.get("db");
        const env = c.get("env");
        const body = (await c.req.json().catch(() => ({}))) as { key?: string };
        const rawKey = body.key?.trim();
        if (!rawKey) {
            return c.json({ ok: false, error: "请输入备份文件完整路径" }, 400);
        }

        const target = await getActiveStorageDriver(env, db);
        if (!target) {
            return c.json({ ok: false, error: "未配置存储桶" }, 400);
        }

        const key = cleanBackupObjectKey(rawKey, target.bucket, target.name);

        if (!key.endsWith(".json")) {
            return c.json({
                ok: false,
                error: `请输入以 .json 结尾的完整文件名，例如：backups/rin-backup-2026-10-24T01-43-36.json`,
            }, 400);
        }

        try {
            const fileRes = await target.driver.get(key);
            if (!fileRes || !fileRes.ok) {
                return c.json({ ok: false, error: `在存储桶中未找到文件: ${key}` }, 404);
            }
            const payload = JSON.parse(await fileRes.text());
            if (!payload || payload.app !== "rin" || !payload.data) {
                return c.json({ ok: false, error: "该文件不是有效的 Rin 备份快照" }, 400);
            }

            return c.json({
                ok: true,
                key,
                filename: key.split("/").pop() || key,
                exportedAt: payload.exportedAt || "未知",
                metadata: payload.metadata || {},
            });
        } catch (error: any) {
            return c.json({ ok: false, error: error.message || "读取备份文件失败" }, 500);
        }
    }));

    // 5. POST /backup/restore-from-storage —— 从存储桶备份恢复
    app.post("/restore-from-storage", adminOnly(async (c: AppContext) => {
        const db = c.get("db");
        const env = c.get("env");
        const cacheManager = c.get("cache");

        const body = (await c.req.json().catch(() => ({}))) as { key?: string };
        const rawKey = body.key?.trim();
        if (!rawKey) {
            return c.json({ ok: false, error: "缺少备份文件路径" }, 400);
        }

        const target = await getActiveStorageDriver(env, db);
        if (!target) {
            return c.json({ ok: false, error: "存储驱动未就绪" }, 400);
        }

        const key = cleanBackupObjectKey(rawKey, target.bucket, target.name);

        try {
            const fileRes = await target.driver.get(key);
            if (!fileRes || !fileRes.ok) {
                return c.json({ ok: false, error: `无法从存储桶读取指定的备份文件: ${key}` }, 404);
            }

            const payload = JSON.parse(await fileRes.text());
            await executeRestore(db, cacheManager, payload);

            return c.json({
                ok: true,
                message: "从云端备份恢复成功！",
                counts: payload.metadata?.counts || {},
            });
        } catch (error: any) {
            return c.json({ ok: false, error: error.message || "恢复失败" }, 500);
        }
    }));

    // 6. POST /backup/import —— 从上传的本地文件恢复
    app.post("/import", adminOnly(async (c: AppContext) => {
        const db = c.get("db");
        const cacheManager = c.get("cache");
        let payload: any;

        const contentType = c.req.header("content-type") || "";
        if (contentType.includes("multipart/form-data")) {
            const form = await c.req.parseBody();
            const file = form.file as File | string | undefined;
            if (!file || typeof file === "string") {
                return c.json({ ok: false, error: "请上传备份文件（.json）" }, 400);
            }
            try {
                payload = JSON.parse(await file.text());
            } catch {
                return c.json({ ok: false, error: "备份文件损坏，无法解析为 JSON" }, 400);
            }
        } else {
            payload = await c.req.json().catch(() => null);
        }

        try {
            await executeRestore(db, cacheManager, payload);
            return c.json({
                ok: true,
                message: "数据恢复成功！",
                counts: payload.metadata?.counts || {},
            });
        } catch (error: any) {
            return c.json({ ok: false, error: error.message || "恢复失败" }, 500);
        }
    }));

    // =========================================================================
    // ★ 以下为 GitHub 专属备份与恢复 API
    // =========================================================================

    // 7. GET /backup/github-config —— 获取 GitHub 备份配置（脱敏）
    app.get("/github-config", adminOnly(async (c: AppContext) => {
        const env = c.get("env");
        const cacheManager = c.get("cache");
        const cfg = await getGitHubBackupConfig(env, cacheManager);

        return c.json({
            ok: true,
            configured: cfg.configured,
            owner: cfg.owner,
            repo: cfg.repo,
            branch: cfg.branch,
            path: cfg.path,
            hasToken: Boolean(cfg.token),
        });
    }));

    // 8. POST /backup/github-config —— 保存/更新 GitHub 备份配置
    app.post("/github-config", adminOnly(async (c: AppContext) => {
        const env = c.get("env");
        const cacheManager = c.get("cache");

        const body = (await c.req.json().catch(() => ({}))) as {
            owner?: string;
            repo?: string;
            branch?: string;
            path?: string;
            token?: string;
        };

        const existing = (await cacheManager.get(GITHUB_BACKUP_CACHE_KEY)) as StoredGitHubConfig | null;

        const owner = (body.owner !== undefined ? body.owner : existing?.owner || "").trim();
        const repo = (body.repo !== undefined ? body.repo : existing?.repo || "").trim();
        const branch = (body.branch !== undefined ? body.branch : existing?.branch || "main").trim() || "main";
        const path = (body.path !== undefined ? body.path : existing?.path || "backups").trim().replace(/^\/+|\/+$/g, "");

        let tokenEnc = existing?.tokenEnc || "";
        if (body.token?.trim()) {
            tokenEnc = await encryptSecret(env, body.token.trim());
        }

        if (!owner || !repo || !tokenEnc) {
            return c.json({ ok: false, error: "请完整填写仓库拥有者 (Owner)、仓库名 (Repo) 以及 GitHub Personal Access Token" }, 400);
        }

        const toSave: StoredGitHubConfig = {
            owner,
            repo,
            branch,
            path: path || "backups",
            tokenEnc,
        };

        await cacheManager.set(GITHUB_BACKUP_CACHE_KEY, toSave);

        return c.json({
            ok: true,
            message: "GitHub 备份配置保存成功",
            configured: true,
            owner,
            repo,
            branch,
            path: toSave.path,
            hasToken: true,
        });
    }));

    // 9. POST /backup/github-test —— 测试 GitHub 仓库连接与权限（自动检测是否为私有仓库）
    app.post("/github-test", adminOnly(async (c: AppContext) => {
        const env = c.get("env");
        const cacheManager = c.get("cache");
        const cfg = await getGitHubBackupConfig(env, cacheManager);

        if (!cfg.configured) {
            return c.json({ ok: false, error: "GitHub 备份尚未配置完成" }, 400);
        }

        try {
            const res = await fetch(`https://api.github.com/repos/${cfg.owner}/${cfg.repo}`, {
                method: "GET",
                headers: {
                    Authorization: `Bearer ${cfg.token}`,
                    Accept: "application/vnd.github+json",
                    "User-Agent": "Rin-Blog-Backup/1.0",
                },
            });

            if (!res.ok) {
                const text = await res.text();
                return c.json({ ok: false, error: `GitHub API 返回错误 (${res.status}): ${text}` }, 400);
            }

            const repoData: any = await res.json();
            return c.json({
                ok: true,
                isPrivate: repoData.private === true,
                defaultBranch: repoData.default_branch,
                message: repoData.private ? "连接成功！目标为私有仓库，符合安全标准。" : "连接成功！⚠️ 警告：当前目标仓库为公开仓库（Public），为保护隐私请强烈建议在 GitHub 设为 Private！",
            });
        } catch (e: any) {
            return c.json({ ok: false, error: `请求 GitHub 失败: ${e.message}` }, 500);
        }
    }));

    // 10. POST /backup/export-to-github —— 导出全量快照并自动 Commit 到 GitHub 仓库
    app.post("/export-to-github", adminOnly(async (c: AppContext) => {
        const db = c.get("db");
        const env = c.get("env");
        const cacheManager = c.get("cache");

        const cfg = await getGitHubBackupConfig(env, cacheManager);
        if (!cfg.configured) {
            return c.json({ ok: false, error: "请先配置并保存 GitHub 备份仓库信息" }, 400);
        }

        const snapshot = await createSnapshot(db);
        const jsonStr = JSON.stringify(snapshot, null, 2);
        const dateStr = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
        const fileName = `rin-backup-${dateStr}.json`;
        const filePath = cfg.path ? `${cfg.path}/${fileName}` : fileName;

        // Base64 编码文件内容（UTF-8 安全）
        const bytes = new TextEncoder().encode(jsonStr);
        let binary = "";
        for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
        const contentBase64 = btoa(binary);

        const commitMessage = `chore: auto backup ${fileName} [feeds:${snapshot.metadata.counts.feeds}, moments:${snapshot.metadata.counts.moments}]`;

        try {
            const putRes = await fetch(`https://api.github.com/repos/${cfg.owner}/${cfg.repo}/contents/${filePath}`, {
                method: "PUT",
                headers: {
                    Authorization: `Bearer ${cfg.token}`,
                    Accept: "application/vnd.github+json",
                    "User-Agent": "Rin-Blog-Backup/1.0",
                    "Content-Type": "application/json",
                },
                body: JSON.stringify({
                    message: commitMessage,
                    content: contentBase64,
                    branch: cfg.branch,
                }),
            });

            if (!putRes.ok) {
                const text = await putRes.text();
                return c.json({ ok: false, error: `GitHub 写入失败 (${putRes.status}): ${text}` }, 400);
            }

            const putData: any = await putRes.json();

            return c.json({
                ok: true,
                filename: fileName,
                path: filePath,
                target: `GitHub (${cfg.owner}/${cfg.repo})`,
                commitSha: putData.commit?.sha?.slice(0, 7),
                htmlUrl: putData.content?.html_url,
                exportedAt: snapshot.exportedAt,
                metadata: snapshot.metadata,
            });
        } catch (e: any) {
            return c.json({ ok: false, error: `提交到 GitHub 异常: ${e.message}` }, 500);
        }
    }));

    // 11. GET /backup/github-list —— 从 GitHub 仓库直接列出所有历史备份快照
    app.get("/github-list", adminOnly(async (c: AppContext) => {
        const env = c.get("env");
        const cacheManager = c.get("cache");
        const cfg = await getGitHubBackupConfig(env, cacheManager);

        if (!cfg.configured) {
            return c.json({ ok: false, error: "未配置 GitHub 备份仓库" }, 400);
        }

        try {
            const url = `https://api.github.com/repos/${cfg.owner}/${cfg.repo}/contents/${cfg.path}?ref=${cfg.branch}`;
            const res = await fetch(url, {
                method: "GET",
                headers: {
                    Authorization: `Bearer ${cfg.token}`,
                    Accept: "application/vnd.github+json",
                    "User-Agent": "Rin-Blog-Backup/1.0",
                },
            });

            if (res.status === 404) {
                // 目录尚未建立
                return c.json({
                    ok: true,
                    target: `${cfg.owner}/${cfg.repo}`,
                    items: [],
                });
            }

            if (!res.ok) {
                const text = await res.text();
                return c.json({ ok: false, error: `读取 GitHub 失败 (${res.status}): ${text}` }, 400);
            }

            const files: any[] = await res.json();
            if (!Array.isArray(files)) {
                return c.json({ ok: true, target: `${cfg.owner}/${cfg.repo}`, items: [] });
            }

            // 筛选出所有以 rin-backup 开头且以 .json 结尾的备份文件，按时间倒序排列
            const backupFiles = files
                .filter((f) => f.type === "file" && f.name.startsWith("rin-backup-") && f.name.endsWith(".json"))
                .map((f) => ({
                    filename: f.name,
                    path: f.path,
                    downloadUrl: f.download_url,
                    size: f.size,
                    sha: f.sha,
                    createdAt: extractDateFromBackupName(f.name),
                }))
                .sort((a, b) => b.filename.localeCompare(a.filename));

            return c.json({
                ok: true,
                target: `${cfg.owner}/${cfg.repo}`,
                branch: cfg.branch,
                items: backupFiles,
            });
        } catch (e: any) {
            return c.json({ ok: false, error: `获取 GitHub 列表失败: ${e.message}` }, 500);
        }
    }));

    // 12. POST /backup/inspect-github-backup —— 下载并核对预览指定 GitHub 备份
    app.post("/inspect-github-backup", adminOnly(async (c: AppContext) => {
        const env = c.get("env");
        const cacheManager = c.get("cache");
        const cfg = await getGitHubBackupConfig(env, cacheManager);

        if (!cfg.configured) {
            return c.json({ ok: false, error: "未配置 GitHub 备份仓库" }, 400);
        }

        const body = (await c.req.json().catch(() => ({}))) as { path?: string };
        const filePath = body.path?.trim();
        if (!filePath) {
            return c.json({ ok: false, error: "缺少文件路径" }, 400);
        }

        try {
            const res = await fetch(`https://api.github.com/repos/${cfg.owner}/${cfg.repo}/contents/${filePath}?ref=${cfg.branch}`, {
                method: "GET",
                headers: {
                    Authorization: `Bearer ${cfg.token}`,
                    Accept: "application/vnd.github.raw+json",
                    "User-Agent": "Rin-Blog-Backup/1.0",
                },
            });

            if (!res.ok) {
                return c.json({ ok: false, error: `无法从 GitHub 仓库读取文件 (${res.status})` }, 404);
            }

            const payload: any = await res.json();
            if (!payload || payload.app !== "rin" || !payload.data) {
                return c.json({ ok: false, error: "该文件不是有效的 Rin 备份文件" }, 400);
            }

            return c.json({
                ok: true,
                path: filePath,
                filename: filePath.split("/").pop() || filePath,
                exportedAt: payload.exportedAt || "未知",
                metadata: payload.metadata || {},
            });
        } catch (e: any) {
            return c.json({ ok: false, error: `解析 GitHub 文件失败: ${e.message}` }, 500);
        }
    }));

    // 13. POST /backup/restore-from-github —— 从 GitHub 仓库恢复全量数据
    app.post("/restore-from-github", adminOnly(async (c: AppContext) => {
        const db = c.get("db");
        const env = c.get("env");
        const cacheManager = c.get("cache");
        const cfg = await getGitHubBackupConfig(env, cacheManager);

        if (!cfg.configured) {
            return c.json({ ok: false, error: "未配置 GitHub 备份仓库" }, 400);
        }

        const body = (await c.req.json().catch(() => ({}))) as { path?: string };
        const filePath = body.path?.trim();
        if (!filePath) {
            return c.json({ ok: false, error: "缺少备份文件路径" }, 400);
        }

        try {
            const res = await fetch(`https://api.github.com/repos/${cfg.owner}/${cfg.repo}/contents/${filePath}?ref=${cfg.branch}`, {
                method: "GET",
                headers: {
                    Authorization: `Bearer ${cfg.token}`,
                    Accept: "application/vnd.github.raw+json",
                    "User-Agent": "Rin-Blog-Backup/1.0",
                },
            });

            if (!res.ok) {
                return c.json({ ok: false, error: `无法下载指定的 GitHub 备份文件 (${res.status})` }, 404);
            }

            const payload: any = await res.json();
            await executeRestore(db, cacheManager, payload);

            return c.json({
                ok: true,
                message: "从 GitHub 备份恢复成功！",
                counts: payload.metadata?.counts || {},
            });
        } catch (e: any) {
            return c.json({ ok: false, error: `恢复失败: ${e.message}` }, 500);
        }
    }));

    return app;
}

function extractDateFromBackupName(name: string): string {
    const match = /rin-backup-([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}-[0-9]{2}-[0-9]{2})/.exec(name);
    if (!match) return "未知时间";
    return match[1].replace(/T/, " ").replace(/-/g, (m, offset) => (offset > 10 ? ":" : "-"));
}
```

---

### 二、`client/src/api/client.ts`（前端 API 完整代码）

在 `client/src/api/client.ts` 中，为 `BackupAPI` 补齐所有关于 GitHub 备份的 API 契约与方法：

```typescript
// API Client for Rin - Type-safe HTTP client to replace Eden
// This client provides a clean, type-safe interface for all backend API endpoints

import { getAuthToken } from "../utils/auth";
import { endpoint } from "../config";

// Import shared types
import type {
  ApiResponse,
  RequestOptions,
  Feed,
  FeedListResponse,
  TimelineItem,
  CreateFeedRequest,
  UpdateFeedRequest,
  AdjacentFeedResponse,
  UserProfile,
  UpdateProfileRequest,
  Tag,
  TagDetail,
  Comment,
  CreateCommentRequest,
  Friend,
  FriendListResponse,
  CreateFriendRequest,
  UpdateFriendRequest,
  Moment,
  CreateMomentRequest,
  ConfigType,
  ConfigResponse,
  AIConfig,
  UploadResponse,
  StorageItem,
  StorageInput,
  StorageListResponse,
  StorageTestResponse,
  AuthStatus,
  LoginRequest,
  LoginResponse,
} from "@rin/api";

export interface SettingsConfigResponse {
  clientConfig: ConfigResponse;
  serverConfig: ConfigResponse;
}

export interface ConfigHealthItem {
  id: string;
  title: {
    key: string;
    values?: Record<string, string | number | boolean>;
  };
  status: "success" | "warning" | "danger";
  configured: boolean;
  impact: {
    key: string;
    values?: Record<string, string | number | boolean>;
  };
  summary: {
    key: string;
    values?: Record<string, string | number | boolean>;
  };
  suggestion?: {
    key: string;
    values?: Record<string, string | number | boolean>;
  };
  details?: Array<{
    key: string;
    values?: Record<string, string | number | boolean>;
  }>;
}

export interface ConfigHealthResponse {
  generatedAt: string;
  summary: Record<"success" | "warning" | "danger", number>;
  items: ConfigHealthItem[];
}

export interface QueueStatusItem {
  id: number;
  title: string | null;
  aiSummaryStatus: "idle" | "pending" | "processing" | "completed" | "failed";
  aiSummaryError: string;
  updatedAt: string;
  createdAt: string;
}

export interface QueueStatusResponse {
  queueConfigured: boolean;
  generatedAt: string;
  summary: Record<"idle" | "pending" | "processing" | "completed" | "failed", number>;
  items: QueueStatusItem[];
}

export interface QueueTaskActionResponse {
  success: boolean;
}

export interface CompatTasksResponse {
  generatedAt: string;
  aiSummary: {
    enabled: boolean;
    queueConfigured: boolean;
    eligible: number;
    forceEligible: number;
  };
  blurhash: {
    eligible: number;
  };
}

export interface CompatAISummaryActionResponse {
  queued: number;
  skipped: number;
  forced: boolean;
}

export interface CompatBlurhashCandidate {
  id: number;
  title: string | null;
  content: string;
}

export interface CompatBlurhashCandidatesResponse {
  generatedAt: string;
  items: CompatBlurhashCandidate[];
}

export interface CompatBlurhashApplyResponse {
  updated: boolean;
}

// 备份相关数据结构
export interface BackupPreviewResponse {
  ok: boolean;
  preview: {
    feedsCount: number;
    commentsCount: number;
    momentsCount: number;
    friendsCount: number;
    storagesCount: number;
  };
  targetStorage: {
    configured: boolean;
    name: string;
    folder: string;
  };
  targetGitHub: {
    configured: boolean;
    repo: string;
    branch: string;
    path: string;
  };
}

export interface BackupExportToStorageResponse {
  ok: boolean;
  filename: string;
  path: string;
  target: string;
  metadata: any;
  totalBackups: number;
  prunedCount: number;
}

export interface BackupStorageListResponse {
  ok: boolean;
  target: string;
  items: Array<{
    key: string;
    filename: string;
    createdAt: string;
    metadata?: any;
  }>;
  error?: string;
}

export interface BackupInspectResponse {
  ok: boolean;
  key: string;
  filename: string;
  exportedAt: string;
  metadata: any;
}

export interface BackupRestoreResponse {
  ok: boolean;
  message: string;
  counts: any;
}

// GitHub 备份配置与列表类型
export interface GitHubBackupConfigResponse {
  ok: boolean;
  configured: boolean;
  owner: string;
  repo: string;
  branch: string;
  path: string;
  hasToken: boolean;
}

export interface GitHubBackupItem {
  filename: string;
  path: string;
  downloadUrl: string;
  size: number;
  sha: string;
  createdAt: string;
}

export interface GitHubBackupListResponse {
  ok: boolean;
  target: string;
  branch: string;
  items: GitHubBackupItem[];
  error?: string;
}

// Re-export for external use
export type {
  ApiResponse,
  RequestOptions,
  Feed,
  FeedListResponse,
  TimelineItem,
  CreateFeedRequest,
  UpdateFeedRequest,
  AdjacentFeedResponse,
  UserProfile,
  UpdateProfileRequest,
  Tag,
  TagDetail,
  Comment,
  CreateCommentRequest,
  Friend,
  FriendListResponse,
  CreateFriendRequest,
  UpdateFriendRequest,
  Moment,
  CreateMomentRequest,
  ConfigType,
  ConfigResponse,
  AIConfig,
  UploadResponse,
  StorageItem,
  StorageInput,
  StorageListResponse,
  StorageTestResponse,
  AuthStatus,
  LoginRequest,
  LoginResponse,
} from "@rin/api";

/**
 * HTTP client for making API requests
 */
class HttpClient {
  constructor(private baseUrl: string) {}

  private async request<T>(
    method: string,
    path: string,
    body?: unknown,
    options?: RequestOptions
  ): Promise<ApiResponse<T>> {
    const url = `${this.baseUrl}${path}`;
    const headers: Record<string, string> = {
      Accept: "application/json",
      ...options?.headers,
    };

    // Add auth token if available
    const token = getAuthToken();
    if (token) {
      headers["Authorization"] = `Bearer ${token}`;
    }

    if (body !== undefined && !(body instanceof FormData)) {
      headers["Content-Type"] = "application/json";
    }

    try {
      const response = await fetch(url, {
        method,
        headers,
        body: body instanceof FormData ? body : body ? JSON.stringify(body) : undefined,
        credentials: "include",
      });

      // Handle 204 No Content
      if (response.status === 204) {
        return { data: undefined as T };
      }

      if (!response.ok) {
        let errorValue: unknown;
        const responseClone = response.clone();
        try {
          errorValue = await response.json();
        } catch {
          errorValue = await responseClone.text();
        }
        let errorMessage: string;
        if (typeof errorValue === 'string') {
          errorMessage = errorValue;
        } else if (errorValue && typeof errorValue === 'object') {
          const err = errorValue as any;
          errorMessage = err.error?.message || err.message || err.error || JSON.stringify(errorValue);
        } else {
          errorMessage = String(errorValue ?? response.statusText);
        }
        return {
          error: {
            status: response.status,
            value: errorMessage,
          },
        };
      }

      const contentLength = response.headers.get("content-length");
      if (contentLength === "0") {
        return { data: undefined as T };
      }

      const contentType = response.headers.get("content-type");
      if (contentType?.includes("application/json")) {
        const data = await response.json();
        return { data };
      }

      const text = await response.text();
      return { data: text as T };
    } catch (error) {
      return {
        error: {
          status: 0,
          value: error instanceof Error ? error.message : "Network error",
        },
      };
    }
  }

  async get<T>(path: string, options?: RequestOptions): Promise<ApiResponse<T>> {
    return this.request<T>("GET", path, undefined, options);
  }

  async post<T>(path: string, body?: unknown, options?: RequestOptions): Promise<ApiResponse<T>> {
    return this.request<T>("POST", path, body, options);
  }

  async put<T>(path: string, body?: unknown, options?: RequestOptions): Promise<ApiResponse<T>> {
    return this.request<T>("PUT", path, body, options);
  }

  async patch<T>(path: string, body?: unknown, options?: RequestOptions): Promise<ApiResponse<T>> {
    return this.request<T>("PATCH", path, body, options);
  }

  async delete<T>(path: string, options?: RequestOptions): Promise<ApiResponse<T>> {
    return this.request<T>("DELETE", path, undefined, options);
  }
}

/**
 * Feed API methods
 */
class FeedAPI {
  constructor(private http: HttpClient) {}

  async list(params?: { page?: number; limit?: number; type?: 'draft' | 'unlisted' | 'normal' }): Promise<ApiResponse<FeedListResponse>> {
    const searchParams = new URLSearchParams();
    if (params?.page) searchParams.set("page", params.page.toString());
    if (params?.limit) searchParams.set("limit", params.limit.toString());
    if (params?.type) searchParams.set("type", params.type);
    
    const query = searchParams.toString();
    return this.http.get<FeedListResponse>(`/api/feed${query ? `?${query}` : ""}`);
  }

  async timeline(): Promise<ApiResponse<TimelineItem[]>> {
    return this.http.get<TimelineItem[]>("/api/feed/timeline");
  }

  async get(id: number | string): Promise<ApiResponse<Feed>> {
    return this.http.get<Feed>(`/api/feed/${id}`);
  }

  async create(body: CreateFeedRequest): Promise<ApiResponse<{ insertedId: number }>> {
    return this.http.post<{ insertedId: number }>("/api/feed", body);
  }

  async update(id: number, body: UpdateFeedRequest): Promise<ApiResponse<void>> {
    return this.http.post<void>(`/api/feed/${id}`, body);
  }

  async delete(id: number): Promise<ApiResponse<void>> {
    return this.http.delete<void>(`/api/feed/${id}`);
  }

  async adjacent(id: number | string): Promise<ApiResponse<AdjacentFeedResponse>> {
    return this.http.get<AdjacentFeedResponse>(`/api/feed/adjacent/${id}`);
  }

  async setTop(id: number, top: number): Promise<ApiResponse<void>> {
    return this.http.post<void>(`/api/feed/top/${id}`, { top });
  }
}

/**
 * Tag API methods
 */
class TagAPI {
  constructor(private http: HttpClient) {}

  async list(): Promise<ApiResponse<Tag[]>> {
    return this.http.get<Tag[]>("/api/tag");
  }

  async get(name: string): Promise<ApiResponse<TagDetail>> {
    return this.http.get<TagDetail>(`/api/tag/${encodeURIComponent(name)}`);
  }
}

/**
 * Comment API methods
 */
class CommentAPI {
  constructor(private http: HttpClient) {}

  async list(feedId: number): Promise<ApiResponse<Comment[]>> {
    return this.http.get<Comment[]>(`/api/comment/${feedId}`);
  }

  async create(feedId: number, body: CreateCommentRequest): Promise<ApiResponse<Comment>> {
    return this.http.post<Comment>(`/api/comment/${feedId}`, body);
  }

  async delete(id: number): Promise<ApiResponse<void>> {
    return this.http.delete<void>(`/api/comment/${id}`);
  }
}

/**
 * User API methods
 */
class UserAPI {
  constructor(private http: HttpClient) {}

  async profile(): Promise<ApiResponse<UserProfile>> {
    return this.http.get<UserProfile>("/api/user/profile");
  }

  async updateProfile(body: UpdateProfileRequest): Promise<ApiResponse<{ success: boolean }>> {
    return this.http.put<{ success: boolean }>("/api/user/profile", body);
  }

  async logout(): Promise<ApiResponse<void>> {
    return this.http.post<void>("/api/user/logout");
  }

  githubAuth(): string {
    return `${endpoint}/api/user/github`;
  }
}

/**
 * Friend API methods
 */
class FriendAPI {
  constructor(private http: HttpClient) {}

  async list(): Promise<ApiResponse<FriendListResponse>> {
    return this.http.get<FriendListResponse>("/api/friend");
  }

  async create(body: CreateFriendRequest): Promise<ApiResponse<Friend>> {
    return this.http.post<Friend>(`/api/friend`, body);
  }

  async update(id: number, body: UpdateFriendRequest): Promise<ApiResponse<Friend>> {
    return this.http.put<Friend>(`/api/friend/${id}`, body);
  }

  async delete(id: number): Promise<ApiResponse<void>> {
    return this.http.delete<void>(`/api/friend/${id}`);
  }
}

/**
 * Moments API methods
 */
class MomentsAPI {
  constructor(private http: HttpClient) {}

  async list(params?: { page?: number; limit?: number }): Promise<ApiResponse<{ data: Moment[]; hasNext: boolean }>> {
    const searchParams = new URLSearchParams();
    if (params?.page) searchParams.set("page", params.page.toString());
    if (params?.limit) searchParams.set("limit", params.limit.toString());
    
    const query = searchParams.toString();
    return this.http.get<{ data: Moment[]; hasNext: boolean }>(`/api/moments${query ? `?${query}` : ""}`);
  }

  async create(body: CreateMomentRequest): Promise<ApiResponse<Moment>> {
    return this.http.post<Moment>("/api/moments", body);
  }

  async update(id: number, body: CreateMomentRequest): Promise<ApiResponse<Moment>> {
    return this.http.post<Moment>(`/api/moments/${id}`, body);
  }

  async delete(id: number): Promise<ApiResponse<void>> {
    return this.http.delete<void>(`/api/moments/${id}`);
  }
}

/**
 * Config API methods
 */
class ConfigAPI {
  constructor(private http: HttpClient) {}

  async getAll(): Promise<ApiResponse<SettingsConfigResponse>> {
    return this.http.get<SettingsConfigResponse>("/api/config");
  }

  async get(type: ConfigType): Promise<ApiResponse<ConfigResponse>> {
    return this.http.get<ConfigResponse>(`/api/config/${type}`);
  }

  async updateAll(body: SettingsConfigResponse): Promise<ApiResponse<SettingsConfigResponse>> {
    return this.http.post<SettingsConfigResponse>("/api/config", body);
  }

  async update(type: ConfigType, body: Record<string, unknown>): Promise<ApiResponse<void>> {
    return this.http.post<void>(`/api/config/${type}`, body);
  }

  async clearCache(): Promise<ApiResponse<void>> {
    return this.http.delete<void>("/api/config/cache");
  }

  async getHealth(): Promise<ApiResponse<ConfigHealthResponse>> {
    return this.http.get<ConfigHealthResponse>("/api/config/health");
  }

  async getQueueStatus(): Promise<ApiResponse<QueueStatusResponse>> {
    return this.http.get<QueueStatusResponse>("/api/config/queue-status");
  }

  async getCompatTasks(): Promise<ApiResponse<CompatTasksResponse>> {
    return this.http.get<CompatTasksResponse>("/api/config/compat-tasks");
  }

  async runCompatAISummary(force = false): Promise<ApiResponse<CompatAISummaryActionResponse>> {
    return this.http.post<CompatAISummaryActionResponse>("/api/config/compat-tasks/ai-summary", { force });
  }

  async getCompatBlurhashCandidates(): Promise<ApiResponse<CompatBlurhashCandidatesResponse>> {
    return this.http.get<CompatBlurhashCandidatesResponse>("/api/config/compat-tasks/blurhash");
  }

  async applyCompatBlurhash(feedId: number, content: string): Promise<ApiResponse<CompatBlurhashApplyResponse>> {
    return this.http.post<CompatBlurhashApplyResponse>(`/api/config/compat-tasks/blurhash/${feedId}`, { content });
  }

  async retryQueueTask(feedId: number): Promise<ApiResponse<QueueTaskActionResponse>> {
    return this.http.post<QueueTaskActionResponse>(`/api/config/queue-status/${feedId}/retry`);
  }

  async deleteQueueTask(feedId: number): Promise<ApiResponse<QueueTaskActionResponse>> {
    return this.http.delete<QueueTaskActionResponse>(`/api/config/queue-status/${feedId}`);
  }

  async testAI(body: {
    provider?: string;
    model?: string;
    api_url?: string;
    api_key?: string;
    testPrompt?: string;
  }): Promise<ApiResponse<{ success: boolean; response?: string; error?: string; details?: string; provider?: string; model?: string }>> {
    return this.http.post<any>("/api/config/test-ai", body);
  }

  async testWebhook(body: {
    webhook_url?: string;
    "webhook.method"?: string;
    "webhook.content_type"?: string;
    "webhook.headers"?: string;
    "webhook.body_template"?: string;
    test_message?: string;
  }): Promise<ApiResponse<{ success: boolean; error?: string; details?: string }>> {
    return this.http.post<{ success: boolean; error?: string; details?: string }>("/api/config/test-webhook", body);
  }
}

/**
 * AI Config API methods (deprecated)
 */
class AIConfigAPI {
  constructor(private http: HttpClient) {}

  async get(): Promise<ApiResponse<AIConfig>> {
    return this.http.get<AIConfig>("/api/ai-config");
  }

  async update(body: Partial<AIConfig>): Promise<ApiResponse<void>> {
    return this.http.post<void>("/api/ai-config", body);
  }
}

export const PROXY_UPLOAD_LIMIT = 95 * 1024 * 1024;
export const MAX_UPLOAD_SIZE = 2 * 1024 * 1024 * 1024;
const DIRECT_UPLOAD_THRESHOLD = 8 * 1024 * 1024;

type PresignResponse =
  | { mode: "proxy" }
  | {
      mode: "direct";
      url: string;
      method: "PUT";
      headers: Record<string, string>;
      token: string;
    };

/**
 * Storage API methods
 */
class StorageAPI {
  constructor(private http: HttpClient) {}

  async uploadViaServer(file: File, key?: string): Promise<ApiResponse<UploadResponse>> {
    const formData = new FormData();
    formData.append("file", file);
    if (key) formData.append("key", key);

    return this.http.post<UploadResponse>("/api/storage", formData);
  }

  async upload(file: File, key?: string): Promise<ApiResponse<UploadResponse>> {
    if (file.size > MAX_UPLOAD_SIZE) {
      return { error: { status: 413, value: "文件不能超过 2GB" } };
    }

    let directError: string | undefined;
    if (file.size >= DIRECT_UPLOAD_THRESHOLD) {
      const presign = await this.http.post<PresignResponse>("/api/storage/presign", {
        name: key || file.name,
        size: file.size,
        type: file.type,
      });
      if (!presign.error && presign.data?.mode === "direct") {
        try {
          const response = await fetch(presign.data.url, {
            method: presign.data.method,
            headers: presign.data.headers,
            body: file,
          });
          if (!response.ok) {
            throw new Error(`存储返回 ${response.status}`);
          }
          return this.http.post<UploadResponse>("/api/storage/complete", { token: presign.data.token });
        } catch (error) {
          directError =
            error instanceof TypeError
              ? "直传失败，请检查存储桶是否已配置 CORS（允许当前站点 PUT 上传）"
              : error instanceof Error
                ? error.message
                : "直传失败";
          console.warn("[storage] direct upload failed, falling back to server upload:", error);
        }
      }
    }

    if (file.size > PROXY_UPLOAD_LIMIT) {
      return {
        error: {
          status: 413,
          value:
            directError ??
            `文件超过 ${Math.floor(PROXY_UPLOAD_LIMIT / 1024 / 1024)}MB，需要在「后台 → 存储管理」添加 S3 / R2 存储后才能上传大文件`,
        },
      };
    }

    return this.uploadViaServer(file, key);
  }
}

/**
 * Storage admin API methods
 */
class StorageAdminAPI {
  constructor(private http: HttpClient) {}

  list(): Promise<ApiResponse<StorageListResponse>> {
    return this.http.get<StorageListResponse>("/api/storages");
  }

  create(body: StorageInput): Promise<ApiResponse<StorageItem>> {
    return this.http.post<StorageItem>("/api/storages", body);
  }

  update(id: number, body: StorageInput): Promise<ApiResponse<StorageItem>> {
    return this.http.put<StorageItem>(`/api/storages/${id}`, body);
  }

  remove(id: number, force = false): Promise<ApiResponse<{ ok: boolean }>> {
    return this.http.delete<{ ok: boolean }>(`/api/storages/${id}${force ? "?force=1" : ""}`);
  }

  test(id: number): Promise<ApiResponse<StorageTestResponse>> {
    return this.http.post<StorageTestResponse>(`/api/storages/${id}/test`, {});
  }

  testConfig(body: StorageInput): Promise<ApiResponse<StorageTestResponse>> {
    return this.http.post<StorageTestResponse>("/api/storages/test-config", body);
  }

  testDefault(): Promise<ApiResponse<StorageTestResponse>> {
    return this.http.post<StorageTestResponse>("/api/storages/test-default", {});
  }
}

/**
 * Backup API methods (全量备份与容灾恢复)
 */
class BackupAPI {
  constructor(private http: HttpClient) {}

  // 1. 获取预备份统计信息
  getPreview(): Promise<ApiResponse<BackupPreviewResponse>> {
    return this.http.get<BackupPreviewResponse>("/api/backup/preview");
  }

  // 2. 本地下载
  async download(): Promise<{ ok: boolean; error?: string; filename?: string }> {
    const token = getAuthToken();
    const headers: Record<string, string> = {};
    if (token) headers["Authorization"] = `Bearer ${token}`;

    try {
      const res = await fetch(`${endpoint}/api/backup/export`, {
        method: "GET",
        headers,
        credentials: "include",
      });

      if (!res.ok) {
        const text = await res.text().catch(() => "");
        return { ok: false, error: text || `下载失败 (${res.status})` };
      }

      const blob = await res.blob();
      const disposition = res.headers.get("content-disposition") || "";
      const match = /filename="?([^"]+)"?/.exec(disposition);
      const filename = match ? match[1] : `rin-backup-${new Date().toISOString().slice(0, 10)}.json`;

      const url = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      window.URL.revokeObjectURL(url);
      return { ok: true, filename };
    } catch (e: any) {
      return { ok: false, error: e.message || "下载失败" };
    }
  }

  // 3. 归档到存储桶
  exportToStorage(): Promise<ApiResponse<BackupExportToStorageResponse>> {
    return this.http.post<BackupExportToStorageResponse>("/api/backup/export-to-storage", {});
  }

  // 4. 获取存储桶备份列表
  listStorageBackups(): Promise<ApiResponse<BackupStorageListResponse>> {
    return this.http.get<BackupStorageListResponse>("/api/backup/storage-list");
  }

  // 4.1 检查存储桶指定路径的备份文件
  inspectStorageBackup(key: string): Promise<ApiResponse<BackupInspectResponse>> {
    return this.http.post<BackupInspectResponse>("/api/backup/inspect-storage-backup", { key });
  }

  // 5. 从存储桶恢复
  restoreFromStorage(key: string): Promise<ApiResponse<BackupRestoreResponse>> {
    return this.http.post<BackupRestoreResponse>("/api/backup/restore-from-storage", { key });
  }

  // 6. 从本地文件恢复
  importFromFile(file: File): Promise<ApiResponse<BackupRestoreResponse>> {
    const formData = new FormData();
    formData.append("file", file);
    return this.http.post<BackupRestoreResponse>("/api/backup/import", formData);
  }

  // =================== GitHub 专属备份能力 ===================

  // 7. 获取 GitHub 备份配置
  getGitHubConfig(): Promise<ApiResponse<GitHubBackupConfigResponse>> {
    return this.http.get<GitHubBackupConfigResponse>("/api/backup/github-config");
  }

  // 8. 保存 GitHub 备份配置
  saveGitHubConfig(body: {
    owner: string;
    repo: string;
    branch?: string;
    path?: string;
    token?: string;
  }): Promise<ApiResponse<{ ok: boolean; message: string }>> {
    return this.http.post<{ ok: boolean; message: string }>("/api/backup/github-config", body);
  }

  // 9. 测试 GitHub 仓库连接与私有属性
  testGitHubConnection(): Promise<ApiResponse<{ ok: boolean; isPrivate: boolean; message: string; defaultBranch?: string }>> {
    return this.http.post<{ ok: boolean; isPrivate: boolean; message: string; defaultBranch?: string }>("/api/backup/github-test", {});
  }

  // 10. 备份到 GitHub 仓库
  exportToGitHub(): Promise<ApiResponse<{
    ok: boolean;
    filename: string;
    path: string;
    target: string;
    commitSha: string;
    htmlUrl: string;
    metadata: any;
  }>> {
    return this.http.post<any>("/api/backup/export-to-github", {});
  }

  // 11. 列出 GitHub 仓库备份列表
  listGitHubBackups(): Promise<ApiResponse<GitHubBackupListResponse>> {
    return this.http.get<GitHubBackupListResponse>("/api/backup/github-list");
  }

  // 12. 预览指定的 GitHub 备份
  inspectGitHubBackup(path: string): Promise<ApiResponse<BackupInspectResponse>> {
    return this.http.post<BackupInspectResponse>("/api/backup/inspect-github-backup", { path });
  }

  // 13. 从 GitHub 仓库恢复
  restoreFromGitHub(path: string): Promise<ApiResponse<BackupRestoreResponse>> {
    return this.http.post<BackupRestoreResponse>("/api/backup/restore-from-github", { path });
  }
}

/**
 * Search API methods
 */
class SearchAPI {
  constructor(private http: HttpClient) {}

  async search(keyword: string, params?: { page?: number; limit?: number }): Promise<ApiResponse<FeedListResponse>> {
    const searchParams = new URLSearchParams();
    if (params?.page) searchParams.set("page", params.page.toString());
    if (params?.limit) searchParams.set("limit", params.limit.toString());

    const query = searchParams.toString();
    return this.http.get<FeedListResponse>(`/api/search/${encodeURIComponent(keyword)}${query ? `?${query}` : ""}`);
  }
}

/**
 * Auth API methods
 */
class AuthAPI {
  constructor(private http: HttpClient) {}

  async status(): Promise<ApiResponse<AuthStatus>> {
    return this.http.get<AuthStatus>("/api/auth/status");
  }

  async login(body: LoginRequest): Promise<ApiResponse<LoginResponse>> {
    return this.http.post<LoginResponse>("/api/auth/login", body);
  }
}

/**
 * WordPress Import API methods
 */
class WordPressAPI {
  constructor(private http: HttpClient) {}

  async import(xml: string): Promise<ApiResponse<{ imported: number }>> {
    return this.http.post<{ imported: number }>("/api/wp", { xml });
  }
}

/**
 * RSS API methods
 */
class RSSAPI {
  constructor(private baseUrl: string) {}

  async getRSS(): Promise<string> {
    const response = await fetch(`${this.baseUrl}/rss.xml`);
    return response.text();
  }

  async getAtom(): Promise<string> {
    const response = await fetch(`${this.baseUrl}/atom.xml`);
    return response.text();
  }

  async getJSON(): Promise<unknown> {
    const response = await fetch(`${this.baseUrl}/rss.json`);
    return response.json();
  }
}

// ============================================================================
// Main API Client Class
// ============================================================================

export class ApiClient {
  private http: HttpClient;
  feed: FeedAPI;
  tag: TagAPI;
  comment: CommentAPI;
  user: UserAPI;
  friend: FriendAPI;
  moments: MomentsAPI;
  config: ConfigAPI;
  aiConfig: AIConfigAPI;
  storage: StorageAPI;
  storages: StorageAdminAPI;
  backup: BackupAPI;
  search: SearchAPI;
  auth: AuthAPI;
  wp: WordPressAPI;
  rss: RSSAPI;

  constructor(baseUrl: string) {
    this.http = new HttpClient(baseUrl);
    this.feed = new FeedAPI(this.http);
    this.tag = new TagAPI(this.http);
    this.comment = new CommentAPI(this.http);
    this.user = new UserAPI(this.http);
    this.friend = new FriendAPI(this.http);
    this.moments = new MomentsAPI(this.http);
    this.config = new ConfigAPI(this.http);
    this.aiConfig = new AIConfigAPI(this.http);
    this.storage = new StorageAPI(this.http);
    this.storages = new StorageAdminAPI(this.http);
    this.backup = new BackupAPI(this.http);
    this.search = new SearchAPI(this.http);
    this.auth = new AuthAPI(this.http);
    this.wp = new WordPressAPI(this.http);
    this.rss = new RSSAPI(baseUrl);
  }
}

export function createClient(baseUrl: string): ApiClient {
  return new ApiClient(baseUrl);
}

export default ApiClient;
```

---

### 三、`client/src/page/settings.tsx`（前端设置页完整代码）

在设置页中加入：
1. **GitHub 异地备份设置面板**（设置 Owner、Repo、Branch、PAT Token、目录路径与连通性测试）。
2. **“备份到 GitHub 私有仓库”** 按钮与对应确认。
3. **“从 GitHub 仓库恢复”** 弹窗，直接列出 GitHub 仓库中的 Commit 快照，选择即可安全恢复。

```tsx
import { Modal, SearchableSelect, SettingsBadge, SettingsCard, SettingsCardBody, SettingsCardHeader, SettingsCardRow } from "@rin/ui";
import { type ChangeEvent, useEffect, useMemo, useRef, useState } from "react";
import { Helmet } from "react-helmet";
import { useTranslation } from "react-i18next";
import ReactLoading from "react-loading";
import { client, oauth_url } from "../app/runtime";
import { Button } from "../components/button";
import { useAlert } from "../components/dialog";
import { HeaderLayoutPreview } from "../components/site-header/layout-preview";
import {
  HEADER_BEHAVIOR_OPTIONS,
  HEADER_LAYOUT_OPTIONS,
  normalizeHeaderBehavior,
  normalizeHeaderLayout,
} from "../components/site-header/layout-options";
import { FEED_CARD_VARIANTS, normalizeFeedCardVariant } from "../components/feed-card-options";
import { FeedCardPreview } from "../components/feed-card-preview";
import { FEED_LAYOUT_OPTIONS, normalizeFeedLayout } from "../components/feed-layout-options";
import { useSiteConfig } from "../hooks/useSiteConfig";
import { applyThemeColor, normalizeThemeColor } from "../utils/theme-color";
import { AISummarySettings } from "./settings-ai";
import { ItemButton, ItemImageInput, ItemInput, ItemSwitch, ItemTitle, ItemWithUpload } from "./settings-items";
import {
  areSettingsDraftsEqual,
  buildAIConfigDraftValue,
  createSettingsConfigWrappers,
  importWordPressFile,
  loadSettingsConfigState,
  mergeSessionConfig,
  saveSettingsConfigState,
  type SettingsDraft,
  updateDraftConfig,
  uploadFavicon,
} from "./settings-helpers";

import "../utils/thumb.css";

const WEBHOOK_METHOD_OPTIONS = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"].map((value) => ({
  label: value,
  value,
}));

const THEME_COLOR_OPTIONS = [
  { label: "Rose", value: "#fc466b" },
  { label: "Violet", value: "#7c3aed" },
  { label: "Blue", value: "#2563eb" },
  { label: "Teal", value: "#0f766e" },
  { label: "Orange", value: "#ea580c" },
];

export function Settings() {
  const { t } = useTranslation();
  const siteConfig = useSiteConfig();
  const [isOpen, setIsOpen] = useState(false);
  const [msg, setMsg] = useState("");
  const [msgList, setMsgList] = useState<{ title: string; reason: string }[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [testingWebhook, setTestingWebhook] = useState(false);
  const [webhookTestMessage, setWebhookTestMessage] = useState("");
  const [draft, setDraft] = useState<SettingsDraft>({ clientConfig: {}, serverConfig: {} });
  const [initialDraft, setInitialDraft] = useState<SettingsDraft>({ clientConfig: {}, serverConfig: {} });
  const [hasStoredAiApiKey, setHasStoredAiApiKey] = useState(false);
  const ref = useRef(false);
  const restoreFileRef = useRef<HTMLInputElement>(null);
  const initialDraftRef = useRef<SettingsDraft>({ clientConfig: {}, serverConfig: {} });
  const { showAlert, AlertUI } = useAlert();

  // ================= 备份 & 恢复交互状态 =================
  const [backupActionLoading, setBackupActionLoading] = useState(false);

  // 1. 备份前确认弹窗
  const [backupConfirmModal, setBackupConfirmModal] = useState<{
    open: boolean;
    type: "local" | "cloud" | "github";
    previewData?: any;
    targetStorage?: any;
    targetGitHub?: any;
  }>({ open: false, type: "local" });

  // 2. 备份成功通知弹窗
  const [backupSuccessModal, setBackupSuccessModal] = useState<{
    open: boolean;
    filename: string;
    path: string;
    target: string;
    counts: any;
    extraNote?: string;
  } | null>(null);

  // 3. 存储桶备份选择列表弹窗
  const [cloudBackupsModal, setCloudBackupsModal] = useState<{
    open: boolean;
    loading: boolean;
    items: any[];
    target: string;
    error?: string;
  }>({ open: false, loading: false, items: [], target: "" });

  const [manualCloudKey, setManualCloudKey] = useState("backups/rin-backup-2026-10-24T01-43-36.json");

  // 3.1 GitHub 备份选择列表弹窗
  const [githubBackupsModal, setGithubBackupsModal] = useState<{
    open: boolean;
    loading: boolean;
    items: any[];
    target: string;
    branch: string;
  }>({ open: false, loading: false, items: [], target: "", branch: "main" });

  // 3.2 GitHub 备份配置管理弹窗
  const [githubConfigModal, setGithubConfigModal] = useState<{
    open: boolean;
    owner: string;
    repo: string;
    branch: string;
    path: string;
    token: string;
    hasToken: boolean;
    configured: boolean;
    saving: boolean;
    testing: boolean;
    testResult?: string;
  }>({
    open: false,
    owner: "",
    repo: "",
    branch: "main",
    path: "backups",
    token: "",
    hasToken: false,
    configured: false,
    saving: false,
    testing: false,
  });

  // 4. 恢复前确认弹窗（包含待恢复数据预览）
  const [restoreConfirmModal, setRestoreConfirmModal] = useState<{
    open: boolean;
    source: "local" | "cloud" | "github";
    localFile?: File | null;
    cloudKey?: string;
    githubPath?: string;
    filename: string;
    exportedAt: string;
    counts: any;
  } | null>(null);

  function getDraftThemeColor(nextDraft: SettingsDraft) {
    return typeof nextDraft.clientConfig["theme.color"] === "string" ? nextDraft.clientConfig["theme.color"] : undefined;
  }

  useEffect(() => {
    if (ref.current) return;
    loadSettingsConfigState()
      .then((state) => {
        setDraft(state.draft);
        setInitialDraft(state.draft);
        initialDraftRef.current = state.draft;
        setHasStoredAiApiKey(state.hasStoredAiApiKey);
        mergeSessionConfig(state.draft.clientConfig);
        applyThemeColor(getDraftThemeColor(state.draft));
      })
      .catch((err: any) => {
        showAlert(t("settings.get_config_failed$message", { message: err.message }));
      })
      .finally(() => {
        setLoading(false);
      });
    ref.current = true;

    return () => {
      applyThemeColor(getDraftThemeColor(initialDraftRef.current));
    };
  }, [showAlert, t]);

  const { clientConfig, serverConfig } = useMemo(() => createSettingsConfigWrappers(draft), [draft]);
  const aiValue = useMemo(() => buildAIConfigDraftValue(draft, hasStoredAiApiKey), [draft, hasStoredAiApiKey]);
  const hasUnsavedChanges = !areSettingsDraftsEqual(draft, initialDraft);
  const themeColorValue = normalizeThemeColor(String(clientConfig.get("theme.color") ?? "#fc466b"));
  const feedLayoutValue = normalizeFeedLayout(String(clientConfig.get("feed.layout") ?? "list"));
  const feedCardVariantValue = normalizeFeedCardVariant(String(clientConfig.get("feed.card_variant") ?? "default"));
  const previewSiteName = String(clientConfig.get("site.name") ?? clientConfig.default("site.name") ?? "Rin");
  const previewSiteAvatar = String(clientConfig.get("site.avatar") ?? clientConfig.default("site.avatar") ?? "");

  function setConfigValue(type: "client" | "server", key: string, value: unknown) {
    setDraft((current) => updateDraftConfig(current, type, key, value));
  }

  function handleReset() {
    setDraft(initialDraft);
    applyThemeColor(getDraftThemeColor(initialDraft));
  }

  async function handleSave() {
    setSaving(true);
    try {
      const state = await saveSettingsConfigState(draft);
      setDraft(state.draft);
      setInitialDraft(state.draft);
      initialDraftRef.current = state.draft;
      setHasStoredAiApiKey(state.hasStoredAiApiKey || aiValue.apiKey.trim().length > 0);
      mergeSessionConfig(state.draft.clientConfig);
      window.dispatchEvent(new Event("storage"));
      showAlert(t("settings.ai_summary.save_success"));
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      showAlert(t("settings.update_failed$message", { message }));
    } finally {
      setSaving(false);
    }
  }

  async function handleFaviconChange(e: ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0];
    if (file) {
      await uploadFavicon(file, t, showAlert);
    }
  }

  async function onFileChange(e: ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0];
    if (file) {
      const { data, error } = await importWordPressFile(file);
      if (data) {
        setMsg(t("settings.import_success$success$skipped", { success: data.imported, skipped: 0 }));
        setMsgList([]);
        setIsOpen(true);
      } else if (error) {
        showAlert(t("settings.import_failed$message", { message: error.value }));
      }
    }
  }

  async function handleTestWebhook() {
    setTestingWebhook(true);
    try {
      const { data, error } = await client.config.testWebhook({
        webhook_url: String(serverConfig.get("webhook_url") ?? ""),
        "webhook.method": String(serverConfig.get("webhook.method") ?? ""),
        "webhook.content_type": String(serverConfig.get("webhook.content_type") ?? ""),
        "webhook.headers": String(serverConfig.get("webhook.headers") ?? ""),
        "webhook.body_template": String(serverConfig.get("webhook.body_template") ?? ""),
        test_message: webhookTestMessage,
      });

      if (error || !data || !data.success) {
        const message = error?.value || data?.error || t("settings.webhook.test.failed");
        const details = data?.details ? `\n${data.details}` : "";
        showAlert(`${message}${details}`);
        return;
      }

      showAlert(t("settings.webhook.test.success"));
    } finally {
      setTestingWebhook(false);
    }
  }

  // ================= 备份 & 恢复处理函数 =================

  // 1. 点击触发前置预览与确认弹窗
  const openBackupConfirm = async (type: "local" | "cloud" | "github") => {
    setBackupActionLoading(true);
    try {
      const { data } = await client.backup.getPreview();
      setBackupConfirmModal({
        open: true,
        type,
        previewData: data?.preview,
        targetStorage: data?.targetStorage,
        targetGitHub: data?.targetGitHub,
      });
    } catch {
      setBackupConfirmModal({ open: true, type });
    } finally {
      setBackupActionLoading(false);
    }
  };

  // 2. 确认开始执行备份
  const executeConfirmedBackup = async () => {
    const type = backupConfirmModal.type;
    setBackupConfirmModal({ ...backupConfirmModal, open: false });
    setBackupActionLoading(true);

    try {
      if (type === "local") {
        const res = await client.backup.download();
        if (!res.ok) {
          showAlert(res.error || "下载备份失败");
        } else {
          setBackupSuccessModal({
            open: true,
            filename: res.filename || "rin-backup-xxxx.json",
            path: "已下载至当前设备浏览器默认下载目录",
            target: "本地设备",
            counts: backupConfirmModal.previewData,
          });
        }
      } else if (type === "cloud") {
        const { data, error } = await client.backup.exportToStorage();
        if (error || !data || !data.ok) {
          showAlert(`存储桶备份失败: ${error?.value || "写入异常"}`);
        } else {
          setBackupSuccessModal({
            open: true,
            filename: data.filename,
            path: data.path,
            target: data.target,
            counts: data.metadata?.counts,
          });
          setManualCloudKey(data.path);
        }
      } else if (type === "github") {
        const { data, error } = await client.backup.exportToGitHub();
        if (error || !data || !data.ok) {
          showAlert(`GitHub 备份失败: ${error?.value || "提交异常，请检查 Token 权限"}`);
        } else {
          setBackupSuccessModal({
            open: true,
            filename: data.filename,
            path: data.path,
            target: data.target,
            counts: data.metadata?.counts,
            extraNote: `Commit SHA: ${data.commitSha}`,
          });
        }
      }
    } catch (e: any) {
      showAlert(`备份过程中出错: ${e.message}`);
    } finally {
      setBackupActionLoading(false);
    }
  };

  // 3. 打开存储桶备份历史选择列表
  const openCloudBackupsList = async () => {
    setCloudBackupsModal({ open: true, loading: true, items: [], target: "" });
    try {
      const { data, error } = await client.backup.listStorageBackups();
      if (error || !data || !data.ok) {
        showAlert(error?.value || "获取存储桶备份列表失败");
        setCloudBackupsModal({ open: false, loading: false, items: [], target: "" });
      } else {
        setCloudBackupsModal({
          open: true,
          loading: false,
          items: data.items || [],
          target: data.target || "云端存储桶",
          error: data.error,
        });
        if (data.items && data.items.length > 0) {
          setManualCloudKey(data.items[0].key);
        }
      }
    } catch (e: any) {
      showAlert(`读取异常: ${e.message}`);
      setCloudBackupsModal({ open: false, loading: false, items: [], target: "" });
    }
  };

  // 3.1 打开 GitHub 备份列表
  const openGitHubBackupsList = async () => {
    setGithubBackupsModal({ open: true, loading: true, items: [], target: "", branch: "main" });
    try {
      const { data, error } = await client.backup.listGitHubBackups();
      if (error || !data || !data.ok) {
        showAlert(error?.value || "获取 GitHub 备份列表失败，请先确认 GitHub 配置是否正确");
        setGithubBackupsModal({ open: false, loading: false, items: [], target: "", branch: "main" });
      } else {
        setGithubBackupsModal({
          open: true,
          loading: false,
          items: data.items || [],
          target: data.target,
          branch: data.branch,
        });
      }
    } catch (e: any) {
      showAlert(`读取 GitHub 异常: ${e.message}`);
      setGithubBackupsModal({ open: false, loading: false, items: [], target: "", branch: "main" });
    }
  };

  // 3.2 打开 GitHub 备份配置弹窗
  const openGitHubConfigDialog = async () => {
    setGithubConfigModal((prev) => ({ ...prev, open: true, testResult: undefined }));
    try {
      const { data } = await client.backup.getGitHubConfig();
      if (data) {
        setGithubConfigModal({
          open: true,
          owner: data.owner || "",
          repo: data.repo || "",
          branch: data.branch || "main",
          path: data.path || "backups",
          token: "",
          hasToken: data.hasToken,
          configured: data.configured,
          saving: false,
          testing: false,
        });
      }
    } catch {}
  };

  // 4. 选择本地 JSON 文件恢复
  const handleSelectLocalFile = async (e: ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    e.target.value = "";
    if (!file) return;

    try {
      const text = await file.text();
      const parsed = JSON.parse(text);
      if (!parsed || parsed.app !== "rin" || !parsed.data) {
        showAlert("无效的备份文件：该文件不是由 Rin 生成的标准数据镜像。");
        return;
      }
      setRestoreConfirmModal({
        open: true,
        source: "local",
        localFile: file,
        filename: file.name,
        exportedAt: parsed.exportedAt || "未知",
        counts: parsed.metadata?.counts || {
          feeds: parsed.data.feeds?.length ?? 0,
          comments: parsed.data.comments?.length ?? 0,
          moments: parsed.data.moments?.length ?? 0,
          friends: parsed.data.friends?.length ?? 0,
        },
      });
    } catch (err: any) {
      showAlert(`读取备份文件出错: ${err.message}`);
    }
  };

  // 5. 点击存储桶快照项核对
  const handleSelectCloudBackupItem = (item: any) => {
    setCloudBackupsModal({ ...cloudBackupsModal, open: false });
    setRestoreConfirmModal({
      open: true,
      source: "cloud",
      cloudKey: item.key,
      filename: item.filename,
      exportedAt: item.createdAt,
      counts: item.metadata?.counts || {},
    });
  };

  // 5.1 点击 GitHub 快照项核对
  const handleSelectGitHubBackupItem = async (item: any) => {
    setGithubBackupsModal({ ...githubBackupsModal, open: false });
    setBackupActionLoading(true);
    try {
      const { data, error } = await client.backup.inspectGitHubBackup(item.path);
      if (error || !data || !data.ok) {
        showAlert(`未能读取 GitHub 备份内容: ${error?.value || "文件解析错误"}`);
      } else {
        setRestoreConfirmModal({
          open: true,
          source: "github",
          githubPath: data.path,
          filename: data.filename,
          exportedAt: data.exportedAt,
          counts: data.metadata?.counts || {},
        });
      }
    } catch (e: any) {
      showAlert(`读取异常: ${e.message}`);
    } finally {
      setBackupActionLoading(false);
    }
  };

  // 6. 最终确认执行恢复操作
  const executeConfirmedRestore = async () => {
    if (!restoreConfirmModal) return;
    const { source, localFile, cloudKey, githubPath } = restoreConfirmModal;
    setRestoreConfirmModal(null);
    setBackupActionLoading(true);

    try {
      let resData: any = null;
      let resError: any = null;

      if (source === "local" && localFile) {
        const { data, error } = await client.backup.importFromFile(localFile);
        resData = data;
        resError = error;
      } else if (source === "cloud" && cloudKey) {
        const { data, error } = await client.backup.restoreFromStorage(cloudKey);
        resData = data;
        resError = error;
      } else if (source === "github" && githubPath) {
        const { data, error } = await client.backup.restoreFromGitHub(githubPath);
        resData = data;
        resError = error;
      }

      if (resError || !resData || !resData.ok) {
        showAlert(`恢复失败: ${resError?.value || resData?.error || "写入异常"}`);
      } else {
        const counts = resData.counts || {};
        showAlert(
          `🎉 全量数据恢复成功！\n成功还原: ${counts.feeds ?? 0} 篇文章、${counts.comments ?? 0} 条评论、${counts.moments ?? 0} 条动态、${counts.friends ?? 0} 个友链。\n点击确定将刷新页面。`,
          () => {
            window.location.reload();
          }
        );
      }
    } catch (err: any) {
      showAlert(`恢复过程出错: ${err.message}`);
    } finally {
      setBackupActionLoading(false);
    }
  };

  return (
    <div className="flex w-full flex-col">
      <Helmet>
        <title>{`${t("settings.title")} - ${siteConfig.name}`}</title>
      </Helmet>
      <main className="w-full rounded-2xl bg-w" aria-label={t("main_content")}>
        <div className="flex flex-col items-start space-y-2">
          {(loading || saving) && <ReactLoading width="1em" height="1em" type="spin" color="#FC466B" />}
          <ItemTitle title={t("settings.site.title")} />
          <ItemInput
            title={t("settings.site.name.title")}
            description={t("settings.site.name.desc")}
            configKeyTitle={t("settings.site.name.label")}
            value={String(clientConfig.get("site.name") ?? "")}
            placeholder={String(clientConfig.default("site.name") ?? t("settings.site.name.label"))}
            onChange={(value) => {
              setConfigValue("client", "site.name", value);
            }}
          />
          <ItemInput
            title={t("settings.site.description.title")}
            description={t("settings.site.description.desc")}
            configKeyTitle={t("settings.site.description.label")}
            value={String(clientConfig.get("site.description") ?? "")}
            placeholder={String(clientConfig.default("site.description") ?? t("settings.site.description.label"))}
            onChange={(value) => {
              setConfigValue("client", "site.description", value);
            }}
          />
          <ItemImageInput
            title={t("settings.site.avatar.title")}
            description={t("settings.site.avatar.desc")}
            configKeyTitle={t("settings.site.avatar.label")}
            value={String(clientConfig.get("site.avatar") ?? "")}
            placeholder={String(clientConfig.default("site.avatar") ?? t("settings.site.avatar.label"))}
            onChange={(value) => {
              setConfigValue("client", "site.avatar", value);
            }}
            onError={showAlert}
          />
          <ItemInput
            title={t("settings.site.page_size.title")}
            description={t("settings.site.page_size.desc")}
            configKeyTitle={t("settings.site.page_size.label")}
            value={String(clientConfig.get("site.page_size") ?? "")}
            placeholder={String(clientConfig.default("site.page_size") ?? t("settings.site.page_size.label"))}
            onChange={(value) => {
              setConfigValue("client", "site.page_size", value);
            }}
          />

          <ItemTitle title={t("settings.personalization.title")} />
          <div className="w-full">
            <SettingsCard>
              <SettingsCardRow
                header={
                  <SettingsCardHeader
                    title={t("settings.header_layout.title")}
                    description={t("settings.header_layout.desc")}
                  />
                }
                action={
                  <SearchableSelect
                    value={normalizeHeaderLayout(String(clientConfig.get("header.layout") ?? "classic"))}
                    onChange={(value) => {
                      setConfigValue("client", "header.layout", value);
                    }}
                    options={HEADER_LAYOUT_OPTIONS.map((value) => ({
                      value,
                      label: t(`settings.header_layout.options.${value}`),
                    }))}
                    placeholder={t("settings.header_layout.title")}
                    emptyLabel={t("no_more")}
                    searchable={false}
                  />
                }
              />
              <SettingsCardBody>
                <div className="grid gap-3 md:grid-cols-2">
                  {HEADER_LAYOUT_OPTIONS.map((value) => (
                    <HeaderLayoutPreview
                      key={value}
                      data={{
                        avatar: previewSiteAvatar,
                        name: previewSiteName,
                        themeColor: themeColorValue,
                      }}
                      layout={value}
                      selected={normalizeHeaderLayout(String(clientConfig.get("header.layout") ?? "classic")) === value}
                      title={t(`settings.header_layout.options.${value}`)}
                      description={t(`settings.header_layout.preview.${value}`)}
                      onClick={() => {
                        setConfigValue("client", "header.layout", value);
                      }}
                    />
                  ))}
                </div>
              </SettingsCardBody>
              <div className="mt-4 border-t border-black/5 pt-4 dark:border-white/10">
                <SettingsCardRow
                  header={
                    <SettingsCardHeader
                      title={t("settings.feed_layout.title")}
                      description={t("settings.feed_layout.desc")}
                    />
                  }
                  action={
                    <SearchableSelect
                      value={feedLayoutValue}
                      onChange={(value) => {
                        setConfigValue("client", "feed.layout", value);
                      }}
                      options={FEED_LAYOUT_OPTIONS.map((value) => ({
                        value,
                        label: t(`settings.feed_layout.options.${value}`),
                      }))}
                      placeholder={t("settings.feed_layout.title")}
                      emptyLabel={t("no_more")}
                      searchable={false}
                    />
                  }
                />
              </div>
              <div className="mt-4 border-t border-black/5 pt-4 dark:border-white/10">
                <SettingsCardRow
                  header={
                    <SettingsCardHeader
                      title={t("settings.feed_card.title")}
                      description={t("settings.feed_card.desc")}
                    />
                  }
                  action={
                    <SearchableSelect
                      value={feedCardVariantValue}
                      onChange={(value) => {
                        setConfigValue("client", "feed.card_variant", value);
                      }}
                      options={FEED_CARD_VARIANTS.map((value) => ({
                        value,
                        label: t(`settings.feed_card.options.${value}`),
                      }))}
                      placeholder={t("settings.feed_card.title")}
                      emptyLabel={t("no_more")}
                      searchable={false}
                    />
                  }
                />
                <SettingsCardBody>
                  <div className="grid gap-3 md:grid-cols-2">
                    {FEED_CARD_VARIANTS.map((value) => (
                      <FeedCardPreview
                        key={value}
                        variant={value}
                        selected={feedCardVariantValue === value}
                        title={t(`settings.feed_card.options.${value}`)}
                        description={t(`settings.feed_card.preview.${value}`)}
                        onClick={() => {
                          setConfigValue("client", "feed.card_variant", value);
                        }}
                      />
                    ))}
                  </div>
                </SettingsCardBody>
              </div>
              <div className="mt-4 border-t border-black/5 pt-4 dark:border-white/10">
                <SettingsCardRow
                  header={
                    <SettingsCardHeader
                      title={t("settings.theme_color.title")}
                      description={t("settings.theme_color.desc")}
                    />
                  }
                  action={
                    <div className="text-sm font-medium t-primary">{themeColorValue}</div>
                  }
                />
                <SettingsCardBody>
                  <div className="flex flex-wrap gap-3">
                    {THEME_COLOR_OPTIONS.map((option) => {
                      const selected = themeColorValue === option.value;
                      return (
                        <button
                          key={option.value}
                          type="button"
                          onClick={() => {
                            setConfigValue("client", "theme.color", option.value);
                            applyThemeColor(option.value);
                          }}
                          className={`flex items-center gap-3 rounded-xl border px-3 py-2 transition-all ${
                            selected
                              ? "border-theme bg-theme/5 shadow-sm shadow-theme/10"
                              : "border-black/10 hover:border-black/20 dark:border-white/10 dark:hover:border-white/20"
                          }`}
                        >
                          <span
                            className="h-6 w-6 rounded-full border border-black/10 dark:border-white/10"
                            style={{ backgroundColor: option.value }}
                          />
                          <span className="text-sm t-primary">{t(`settings.theme_color.options.${option.label.toLowerCase()}`)}</span>
                          {selected ? <i className="ri-check-line text-theme" /> : null}
                        </button>
                      );
                    })}
                    <label className="flex items-center gap-3 rounded-xl border border-black/10 px-3 py-2 hover:border-black/20 dark:border-white/10 dark:hover:border-white/20">
                      <input
                        type="color"
                        value={themeColorValue}
                        onChange={(event) => {
                          const normalized = normalizeThemeColor(event.target.value);
                          setConfigValue("client", "theme.color", normalized);
                          applyThemeColor(normalized);
                        }}
                        className="color-input-reset h-6 w-6 cursor-pointer rounded-full border-0 bg-transparent p-0"
                      />
                      <span className="text-sm t-primary">{t("settings.theme_color.custom")}</span>
                    </label>
                  </div>
                </SettingsCardBody>
              </div>
              <div className="mt-4 border-t border-black/5 pt-4 dark:border-white/10">
                <SettingsCardRow
                  header={
                    <SettingsCardHeader
                      title={t("settings.header_behavior.title")}
                      description={t("settings.header_behavior.desc")}
                    />
                  }
                  action={
                    <SearchableSelect
                      value={normalizeHeaderBehavior(String(clientConfig.get("header.behavior") ?? "fixed"))}
                      onChange={(value) => {
                        setConfigValue("client", "header.behavior", value);
                      }}
                      options={HEADER_BEHAVIOR_OPTIONS.map((value) => ({
                        value,
                        label: t(`settings.header_behavior.options.${value}`),
                      }))}
                      placeholder={t("settings.header_behavior.title")}
                      emptyLabel={t("no_more")}
                      searchable={false}
                    />
                  }
                />
              </div>
            </SettingsCard>
          </div>

          <ItemTitle title={t("settings.other.title")} />
          <ItemSwitch
            title={t("settings.login.enable.title")}
            description={t("settings.login.enable.desc", { url: oauth_url })}
            checked={clientConfig.getBoolean("login.enabled")}
            onChange={(checked) => {
              setConfigValue("client", "login.enabled", checked);
            }}
          />
          <ItemSwitch
            title={t("settings.comment.enable.title")}
            description={t("settings.comment.enable.desc")}
            checked={clientConfig.getBoolean("comment.enabled")}
            onChange={(checked) => {
              setConfigValue("client", "comment.enabled", checked);
            }}
          />
          <ItemSwitch
            title={t("settings.counter.enable.title")}
            description={t("settings.counter.enable.desc")}
            checked={clientConfig.getBoolean("counter.enabled")}
            onChange={(checked) => {
              setConfigValue("client", "counter.enabled", checked);
            }}
          />
          <ItemSwitch
            title={t("settings.rss.title")}
            description={t("settings.rss.desc")}
            checked={clientConfig.getBoolean("rss")}
            onChange={(checked) => {
              setConfigValue("client", "rss", checked);
            }}
          />
          <ItemWithUpload
            title={t("settings.favicon.title")}
            description={t("settings.favicon.desc")}
            accept="image/jpeg,image/png,image/gif,image/webp,image/svg+xml"
            onFileChange={handleFaviconChange}
          />
          <ItemInput
            title={t("settings.footer.title")}
            description={t("settings.footer.desc")}
            configKeyTitle="Footer HTML"
            value={String(clientConfig.get("footer") ?? "")}
            onChange={(value) => {
              setConfigValue("client", "footer", value);
            }}
          />

          <ItemTitle title={t("settings.webhook.title")} />
          <ItemInput
            title={t("settings.webhook.url.title")}
            description={t("settings.webhook.url.desc")}
            configKeyTitle="WEBHOOK_URL"
            value={String(serverConfig.get("webhook_url") ?? "")}
            placeholder="https://example.com/webhook"
            onChange={(value) => {
              setConfigValue("server", "webhook_url", value);
            }}
          />
          <div className="w-full">
            <SettingsCard>
              <SettingsCardRow
                header={
                  <SettingsCardHeader
                    title={t("settings.webhook.method.title")}
                    description={t("settings.webhook.method.desc")}
                  />
                }
                action={
                  <SearchableSelect
                    value={String(serverConfig.get("webhook.method") ?? "")}
                    onChange={(value) => {
                      setConfigValue("server", "webhook.method", value);
                    }}
                    options={WEBHOOK_METHOD_OPTIONS}
                    placeholder={String(serverConfig.default("webhook.method") ?? "POST")}
                    searchPlaceholder={t("settings.webhook.method.title")}
                    emptyLabel={t("no_more")}
                    allowCustomValue
                    customValueLabel={(value) => `${t("update.title")}: ${value}`}
                  />
                }
              />
            </SettingsCard>
          </div>
          <ItemInput
            title={t("settings.webhook.content_type.title")}
            description={t("settings.webhook.content_type.desc")}
            configKeyTitle="Content-Type"
            value={String(serverConfig.get("webhook.content_type") ?? "")}
            placeholder={String(serverConfig.default("webhook.content_type") ?? "application/json")}
            onChange={(value) => {
              setConfigValue("server", "webhook.content_type", value);
            }}
          />
          <ItemInput
            title={t("settings.webhook.headers.title")}
            description={t("settings.webhook.headers.desc")}
            configKeyTitle={t("settings.webhook.headers.label")}
            value={String(serverConfig.get("webhook.headers") ?? "")}
            placeholder={String(serverConfig.default("webhook.headers") ?? "{}")}
            onChange={(value) => {
              setConfigValue("server", "webhook.headers", value);
            }}
          />
          <ItemInput
            title={t("settings.webhook.body_template.title")}
            description={t("settings.webhook.body_template.desc")}
            configKeyTitle={t("settings.webhook.body_template.label")}
            value={String(serverConfig.get("webhook.body_template") ?? "")}
            placeholder={String(serverConfig.default("webhook.body_template") ?? "")}
            onChange={(value) => {
              setConfigValue("server", "webhook.body_template", value);
            }}
          />
          <div className="w-full">
            <SettingsCard>
              <SettingsCardRow
                header={
                  <SettingsCardHeader
                    title={t("settings.webhook.test.title")}
                    description={t("settings.webhook.test.desc")}
                  />
                }
                action={
                  <Button
                    title={testingWebhook ? t("settings.webhook.test.sending") : t("settings.webhook.test.button")}
                    onClick={handleTestWebhook}
                    disabled={testingWebhook}
                  />
                }
              />
              <SettingsCardBody>
                <textarea
                  value={webhookTestMessage}
                  placeholder={t("settings.webhook.test.placeholder")}
                  onChange={(event) => {
                    setWebhookTestMessage(event.target.value);
                  }}
                  className="min-h-28 w-full rounded-xl border border-black/10 bg-w px-4 py-3 text-sm t-primary outline-none transition-colors placeholder:text-neutral-400 focus:border-black/20 focus:ring-2 focus:ring-theme/10 dark:border-white/10 dark:placeholder:text-neutral-500 dark:focus:border-white/20"
                />
              </SettingsCardBody>
            </SettingsCard>
          </div>

          <ItemTitle title={t("settings.friend.title")} />
          <ItemSwitch
            title={t("settings.friend.apply.title")}
            description={t("settings.friend.apply.desc")}
            checked={Boolean(clientConfig.get("friend_apply_enable"))}
            onChange={(checked) => {
              setConfigValue("client", "friend_apply_enable", checked);
            }}
          />
          <ItemSwitch
            title={t("settings.friend.health.title")}
            description={t("settings.friend.health.desc")}
            checked={Boolean(serverConfig.get("friend_crontab"))}
            onChange={(checked) => {
              setConfigValue("server", "friend_crontab", checked);
            }}
          />
          <ItemInput
            title={t("settings.friend.health.ua.title")}
            description={t("settings.friend.health.ua.desc")}
            configKeyTitle="User-Agent"
            value={String(serverConfig.get("friend_ua") ?? "")}
            placeholder={String(serverConfig.default("friend_ua") ?? "User-Agent")}
            onChange={(value) => {
              setConfigValue("server", "friend_ua", value);
            }}
          />

          <ItemTitle title={t("settings.maintenance.title")} />

          {/* ============ ★ GitHub 异地私有仓库备份与恢复 ============ */}
          <div className="w-full">
            <SettingsCard tone="default">
              <SettingsCardRow
                header={
                  <SettingsCardHeader
                    title="GitHub 私有仓库自动备份（极度推荐 • 异地容灾）"
                    description="将博客所有数据直接作为一个版本提交到您的 GitHub 私有仓库中。完全独立于 Cloudflare 架构，具备永久提交历史，即便主机或域名丢失，数据依然安全无虞。"
                    badge={<SettingsBadge tone="success">跨云独立容灾</SettingsBadge>}
                  />
                }
                action={
                  <div className="flex flex-wrap gap-2">
                    <Button
                      secondary
                      title="配置 GitHub 仓库"
                      onClick={openGitHubConfigDialog}
                    />
                    <Button
                      title={backupActionLoading ? "请稍候..." : "备份到 GitHub 仓库"}
                      onClick={() => openBackupConfirm("github")}
                      disabled={backupActionLoading}
                    />
                    <Button
                      secondary
                      title="从 GitHub 恢复"
                      onClick={openGitHubBackupsList}
                      disabled={backupActionLoading}
                    />
                  </div>
                }
              />
            </SettingsCard>
          </div>

          {/* ============ ★ 一键全量数据备份模块 ============ */}
          <div className="w-full">
            <SettingsCard>
              <SettingsCardRow
                header={
                  <SettingsCardHeader
                    title="数据全量备份（本地 / 存储桶）"
                    description="将所有文章、草稿、分类、评论、动态、友链、站点设置打包。支持直接下载到手机/电脑，或存入已绑定的存储桶（保留最近 3 份）。"
                  />
                }
                action={
                  <div className="flex flex-wrap gap-2">
                    <Button
                      title={backupActionLoading ? "请稍候..." : "下载备份到本地"}
                      onClick={() => openBackupConfirm("local")}
                      disabled={backupActionLoading}
                    />
                    <Button
                      secondary
                      title={backupActionLoading ? "请稍候..." : "备份到存储桶"}
                      onClick={() => openBackupConfirm("cloud")}
                      disabled={backupActionLoading}
                    />
                  </div>
                }
              />
            </SettingsCard>
          </div>

          {/* ============ ★ 全量灾难恢复模块（本地 / 存储桶） ============ */}
          <div className="w-full">
            <SettingsCard tone="warning">
              <SettingsCardRow
                header={
                  <SettingsCardHeader
                    title="数据恢复与灾难迁移（本地 / 存储桶）"
                    description="从本地 .json 文件恢复，或从当前绑定的存储桶恢复历史快照。"
                    badge={<SettingsBadge tone="warning">覆盖式写入</SettingsBadge>}
                  />
                }
                action={
                  <div className="flex flex-wrap gap-2">
                    <input
                      ref={restoreFileRef}
                      type="file"
                      accept=".json,application/json"
                      className="hidden"
                      onChange={handleSelectLocalFile}
                    />
                    <Button
                      title="从本地文件恢复"
                      onClick={() => restoreFileRef.current?.click()}
                      disabled={backupActionLoading}
                    />
                    <Button
                      secondary
                      title="从存储桶恢复"
                      onClick={openCloudBackupsList}
                      disabled={backupActionLoading}
                    />
                  </div>
                }
              />
            </SettingsCard>
          </div>

          <ItemSwitch
            title={t("settings.cache.enabled.title")}
            description={t("settings.cache.enabled.desc")}
            checked={Boolean(clientConfig.getBoolean("cache.enabled"))}
            onChange={(checked) => {
              setConfigValue("client", "cache.enabled", checked);
            }}
          />
          <ItemButton
            title={t("settings.cache.clear.title")}
            description={t("settings.cache.clear.desc")}
            buttonTitle={t("clear")}
            onConfirm={async () => {
              await client.config.clearCache().then(({ error }) => {
                if (error) {
                  showAlert(t("settings.cache.clear_failed$message", { message: error.value }));
                }
              });
            }}
            alertTitle={t("settings.cache.clear.confirm.title")}
            alertDescription={t("settings.cache.clear.confirm.desc")}
          />
          <ItemWithUpload
            title={t("settings.wordpress.title")}
            description={t("settings.wordpress.desc")}
            accept="application/xml"
            onFileChange={onFileChange}
          />

          <AISummarySettings
            value={aiValue}
            onChange={(updates) => {
              if (updates.enabled !== undefined) {
                setConfigValue("server", "ai_summary.enabled", updates.enabled);
              }
              if (updates.provider !== undefined) {
                setConfigValue("server", "ai_summary.provider", updates.provider);
              }
              if (updates.model !== undefined) {
                setConfigValue("server", "ai_summary.model", updates.model);
              }
              if (updates.apiUrl !== undefined) {
                setConfigValue("server", "ai_summary.api_url", updates.apiUrl);
              }
              if (updates.apiKey !== undefined) {
                setConfigValue("server", "ai_summary.api_key", updates.apiKey);
              }
            }}
          />

          {hasUnsavedChanges && (
            <div className="sticky bottom-4 z-20 mt-6 w-full pb-2">
              <SettingsCard tone="warning">
              <SettingsCardRow
                header={
                  <SettingsCardHeader
                    title={t("settings.ai_summary.save.title")}
                    description={t("settings.ai_summary.unsaved_changes")}
                    badge={<SettingsBadge tone="warning">{t("settings.ai_summary.unsaved_changes")}</SettingsBadge>}
                  />
                }
                action={
                  <>
                    <Button secondary title={t("reset")} onClick={handleReset} disabled={saving} />
                    <Button title={t("save")} onClick={handleSave} disabled={saving || loading} />
                  </>
                }
              />
              </SettingsCard>
            </div>
          )}
        </div>
      </main>

      {/* 1. 备份前确认弹窗 */}
      <Modal
        isOpen={backupConfirmModal.open}
        onRequestClose={() => setBackupConfirmModal({ ...backupConfirmModal, open: false })}
        contentLabel="确认备份"
        panelClassName="p-6"
      >
        <div className="flex flex-col items-start w-full">
          <h1 className="text-xl font-bold tracking-[-0.02em] t-primary">
            {backupConfirmModal.type === "local"
              ? "确认导出本地备份？"
              : backupConfirmModal.type === "github"
                ? "确认备份到 GitHub 私有仓库？"
                : "确认备份到云端存储桶？"}
          </h1>
          <p className="mt-2 text-sm text-neutral-500">
            {backupConfirmModal.type === "local"
              ? "系统将把博客全部数据打包生成为一个 JSON 快照，并通过浏览器直接下载到您的电脑或手机中。"
              : backupConfirmModal.type === "github"
                ? "系统将把博客全部数据打包成快照，并通过 GitHub API 自动提交（Commit）到您绑定的 GitHub 私有仓库中。"
                : "系统将把博客全部数据打包，并上传归档到您当前已绑定的云端存储桶中（自动保留最新的 3 份备份，旧备份自动清理淘汰）。"}
          </p>

          <div className="mt-4 w-full rounded-xl bg-secondary p-4 space-y-2 text-sm t-primary">
            <p className="font-semibold text-xs uppercase tracking-wider text-theme">即将备份的数据概览：</p>
            <div className="grid grid-cols-2 gap-2 pt-1 text-xs sm:text-sm">
              <p>📄 文章数量: <span className="font-semibold">{backupConfirmModal.previewData?.feedsCount ?? "计算中..."}</span> 篇</p>
              <p>💬 评论条数: <span className="font-semibold">{backupConfirmModal.previewData?.commentsCount ?? "计算中..."}</span> 条</p>
              <p>⚡ 动态数量: <span className="font-semibold">{backupConfirmModal.previewData?.momentsCount ?? "计算中..."}</span> 条</p>
              <p>🤝 友链数量: <span className="font-semibold">{backupConfirmModal.previewData?.friendsCount ?? "计算中..."}</span> 个</p>
              <p>💾 存储配置: <span className="font-semibold">{backupConfirmModal.previewData?.storagesCount ?? "计算中..."}</span> 个</p>
            </div>
            <div className="pt-2 border-t border-black/5 dark:border-white/5 text-xs text-neutral-500">
              <p>📍 备份目标: <span className="font-semibold t-primary">
                {backupConfirmModal.type === "local"
                  ? "本地设备（浏览器下载文件）"
                  : backupConfirmModal.type === "github"
                    ? `GitHub 仓库: ${backupConfirmModal.targetGitHub?.repo || "未配置"}`
                    : `${backupConfirmModal.targetStorage?.name || "绑定的存储桶"} -> ${backupConfirmModal.targetStorage?.folder || "backups"}/`}
              </span></p>
            </div>
          </div>

          <div className="mt-6 flex w-full justify-end gap-2">
            <Button secondary title="取消" onClick={() => setBackupConfirmModal({ ...backupConfirmModal, open: false })} />
            <Button
              title={backupActionLoading ? "正在打包..." : "确认开始备份"}
              onClick={executeConfirmedBackup}
              disabled={backupActionLoading}
            />
          </div>
        </div>
      </Modal>

      {/* 2. 备份成功信息回执弹窗 */}
      <Modal
        isOpen={Boolean(backupSuccessModal?.open)}
        onRequestClose={() => setBackupSuccessModal(null)}
        contentLabel="备份成功"
        panelClassName="p-6"
      >
        {backupSuccessModal ? (
          <div className="flex flex-col items-start w-full">
            <div className="flex items-center gap-2 text-emerald-600 dark:text-emerald-400">
              <i className="ri-checkbox-circle-fill text-2xl" />
              <h1 className="text-xl font-bold tracking-[-0.02em] t-primary">备份打包成功！</h1>
            </div>
            <p className="mt-2 text-sm text-neutral-500">您的数据已经生成并安全写入，详细信息如下：</p>

            <div className="mt-4 w-full rounded-xl bg-secondary p-4 space-y-2 text-xs sm:text-sm t-primary">
              <p>🏷️ <b>备份文件名：</b> <span className="font-mono">{backupSuccessModal.filename}</span></p>
              <p>📦 <b>目标归档：</b> <span>{backupSuccessModal.target}</span></p>
              <p>📍 <b>存储路径：</b> <span className="font-mono break-all">{backupSuccessModal.path}</span></p>
              {backupSuccessModal.extraNote ? (
                <p>🔗 <b>附加信息：</b> <span className="font-mono">{backupSuccessModal.extraNote}</span></p>
              ) : null}
              {backupSuccessModal.counts ? (
                <div className="pt-2 border-t border-black/5 dark:border-white/5 grid grid-cols-2 gap-1 text-neutral-500">
                  <span>包含文章: {backupSuccessModal.counts.feeds ?? 0} 篇</span>
                  <span>包含评论: {backupSuccessModal.counts.comments ?? 0} 条</span>
                  <span>包含动态: {backupSuccessModal.counts.moments ?? 0} 条</span>
                  <span>包含友链: {backupSuccessModal.counts.friends ?? 0} 个</span>
                </div>
              ) : null}
            </div>

            <div className="mt-6 flex w-full justify-end">
              <Button title="好的，我知道了" onClick={() => setBackupSuccessModal(null)} />
            </div>
          </div>
        ) : null}
      </Modal>

      {/* 3.1 GitHub 备份配置管理弹窗 */}
      <Modal
        isOpen={githubConfigModal.open}
        onRequestClose={() => setGithubConfigModal({ ...githubConfigModal, open: false })}
        contentLabel="配置 GitHub 私有备份仓库"
        size="md"
        panelClassName="p-6"
      >
        <div className="flex flex-col items-start w-full gap-3">
          <div className="flex items-center gap-2">
            <i className="ri-github-fill text-2xl t-primary" />
            <h1 className="text-xl font-bold tracking-[-0.02em] t-primary">GitHub 私有仓库备份设置</h1>
          </div>
          <p className="text-xs text-neutral-500">
            建议在 GitHub 创建一个 <b>Private（私有）</b> 仓库用于备份。访问令牌（PAT）将使用 AES-GCM 强加密保存在数据库中。
          </p>

          <div className="w-full space-y-3 mt-2">
            <div>
              <label className="text-xs font-semibold t-primary block mb-1">GitHub 用户名或组织名 (Owner)</label>
              <input
                type="text"
                placeholder="例如：xmjerssss"
                value={githubConfigModal.owner}
                onChange={(e) => setGithubConfigModal({ ...githubConfigModal, owner: e.target.value })}
                className="w-full rounded-xl border border-black/10 bg-w px-3 py-2 text-sm t-primary outline-none focus:border-theme dark:border-white/10"
              />
            </div>

            <div>
              <label className="text-xs font-semibold t-primary block mb-1">仓库名称 (Repo)</label>
              <input
                type="text"
                placeholder="例如：blog-backups (必须为 Private 仓库)"
                value={githubConfigModal.repo}
                onChange={(e) => setGithubConfigModal({ ...githubConfigModal, repo: e.target.value })}
                className="w-full rounded-xl border border-black/10 bg-w px-3 py-2 text-sm t-primary outline-none focus:border-theme dark:border-white/10"
              />
            </div>

            <div className="grid grid-cols-2 gap-2">
              <div>
                <label className="text-xs font-semibold t-primary block mb-1">分支 (Branch)</label>
                <input
                  type="text"
                  placeholder="main"
                  value={githubConfigModal.branch}
                  onChange={(e) => setGithubConfigModal({ ...githubConfigModal, branch: e.target.value })}
                  className="w-full rounded-xl border border-black/10 bg-w px-3 py-2 text-sm t-primary outline-none focus:border-theme dark:border-white/10"
                />
              </div>
              <div>
                <label className="text-xs font-semibold t-primary block mb-1">目录路径 (Path)</label>
                <input
                  type="text"
                  placeholder="backups"
                  value={githubConfigModal.path}
                  onChange={(e) => setGithubConfigModal({ ...githubConfigModal, path: e.target.value })}
                  className="w-full rounded-xl border border-black/10 bg-w px-3 py-2 text-sm t-primary outline-none focus:border-theme dark:border-white/10"
                />
              </div>
            </div>

            <div>
              <label className="text-xs font-semibold t-primary block mb-1">
                GitHub 个人访问令牌 (Personal Access Token)
                {githubConfigModal.hasToken ? (
                  <span className="ml-2 text-emerald-500 font-normal">（已加密保存，留空为不修改）</span>
                ) : null}
              </label>
              <input
                type="password"
                placeholder={githubConfigModal.hasToken ? "••••••••••••••••" : "ghp_xxxxxxxxxxxxxxxxxxxx"}
                value={githubConfigModal.token}
                onChange={(e) => setGithubConfigModal({ ...githubConfigModal, token: e.target.value })}
                className="w-full rounded-xl border border-black/10 bg-w px-3 py-2 text-sm font-mono t-primary outline-none focus:border-theme dark:border-white/10"
              />
              <p className="text-[11px] text-neutral-400 mt-1">
                需要具有 <b>repo</b> 权限（或细粒度 Token 赋予目标私有仓库的 <b>Contents: Read and write</b> 权限）。
              </p>
            </div>

            {githubConfigModal.testResult ? (
              <p className="text-xs p-3 rounded-lg bg-secondary text-neutral-600 dark:text-neutral-300 font-mono">
                {githubConfigModal.testResult}
              </p>
            ) : null}
          </div>

          <div className="mt-4 flex w-full justify-between items-center">
            <Button
              secondary
              title={githubConfigModal.testing ? "测试中..." : "测试连接"}
              disabled={githubConfigModal.testing || githubConfigModal.saving}
              onClick={async () => {
                setGithubConfigModal((prev) => ({ ...prev, testing: true, testResult: "正在连接 GitHub 验证..." }));
                try {
                  const { data, error } = await client.backup.testGitHubConnection();
                  if (error || !data || !data.ok) {
                    setGithubConfigModal((prev) => ({
                      ...prev,
                      testing: false,
                      testResult: `❌ 失败: ${error?.value || "无法连接仓库，请检查 Token 权限"}`,
                    }));
                  } else {
                    setGithubConfigModal((prev) => ({
                      ...prev,
                      testing: false,
                      testResult: `✅ ${data.message}`,
                    }));
                  }
                } catch (e: any) {
                  setGithubConfigModal((prev) => ({ ...prev, testing: false, testResult: `❌ 异常: ${e.message}` }));
                }
              }}
            />

            <div className="flex gap-2">
              <Button secondary title="取消" onClick={() => setGithubConfigModal({ ...githubConfigModal, open: false })} />
              <Button
                title={githubConfigModal.saving ? "保存中..." : "保存配置"}
                disabled={githubConfigModal.saving || githubConfigModal.testing}
                onClick={async () => {
                  if (!githubConfigModal.owner.trim() || !githubConfigModal.repo.trim()) {
                    showAlert("请填写完整 GitHub 用户名和仓库名");
                    return;
                  }
                  if (!githubConfigModal.hasToken && !githubConfigModal.token.trim()) {
                    showAlert("请输入 GitHub Personal Access Token");
                    return;
                  }
                  setGithubConfigModal((prev) => ({ ...prev, saving: true }));
                  try {
                    const { data, error } = await client.backup.saveGitHubConfig({
                      owner: githubConfigModal.owner.trim(),
                      repo: githubConfigModal.repo.trim(),
                      branch: githubConfigModal.branch.trim(),
                      path: githubConfigModal.path.trim(),
                      token: githubConfigModal.token.trim() || undefined,
                    });
                    if (error || !data || !data.ok) {
                      showAlert(`保存失败: ${error?.value || "写入异常"}`);
                    } else {
                      showAlert("GitHub 备份配置保存成功！");
                      setGithubConfigModal((prev) => ({ ...prev, open: false, hasToken: true, configured: true }));
                    }
                  } catch (e: any) {
                    showAlert(`保存异常: ${e.message}`);
                  } finally {
                    setGithubConfigModal((prev) => ({ ...prev, saving: false }));
                  }
                }}
              />
            </div>
          </div>
        </div>
      </Modal>

      {/* 3.2 GitHub 备份选择列表弹窗 */}
      <Modal
        isOpen={githubBackupsModal.open}
        onRequestClose={() => setGithubBackupsModal({ ...githubBackupsModal, open: false })}
        contentLabel="从 GitHub 仓库恢复"
        size="lg"
        panelClassName="p-6"
      >
        <div className="flex flex-col items-start w-full">
          <div className="flex items-center gap-2">
            <i className="ri-github-fill text-2xl t-primary" />
            <h1 className="text-xl font-bold tracking-[-0.02em] t-primary">从 GitHub 仓库恢复数据</h1>
          </div>
          <p className="mt-1 text-sm text-neutral-500">
            仓库 <b>{githubBackupsModal.target}</b>（分支: {githubBackupsModal.branch}）中保存的快照：
          </p>

          <div className="mt-4 w-full max-h-80 overflow-y-auto space-y-3">
            {githubBackupsModal.loading ? (
              <div className="flex items-center justify-center p-8 text-neutral-400 gap-2">
                <ReactLoading type="spin" color="#FC466B" width={20} height={20} />
                <span>正在获取 GitHub 仓库快照...</span>
              </div>
            ) : githubBackupsModal.items.length === 0 ? (
              <div className="p-4 rounded-xl bg-secondary text-sm text-neutral-500">
                <p>该仓库指定目录中暂未发现任何备份快照文件（以 rin-backup- 开头的 json 文件）。</p>
              </div>
            ) : (
              githubBackupsModal.items.map((item, idx) => (
                <div
                  key={item.sha || idx}
                  className="flex flex-col sm:flex-row sm:items-center justify-between p-4 rounded-xl border border-black/10 dark:border-white/10 bg-secondary gap-3"
                >
                  <div className="min-w-0">
                    <p className="font-mono text-sm font-semibold truncate t-primary">{item.filename}</p>
                    <p className="text-xs text-neutral-400 mt-1">
                      备份时间: {item.createdAt} · 大小: {Math.round((item.size || 0) / 1024)} KB · Commit SHA: {item.sha?.slice(0, 7)}
                    </p>
                  </div>
                  <Button
                    title="选择此备份"
                    onClick={() => handleSelectGitHubBackupItem(item)}
                  />
                </div>
              ))
            )}
          </div>

          <div className="mt-6 flex w-full justify-end">
            <Button secondary title="关闭" onClick={() => setGithubBackupsModal({ ...githubBackupsModal, open: false })} />
          </div>
        </div>
      </Modal>

      {/* 3.3 存储桶备份历史选择列表弹窗 */}
      <Modal
        isOpen={cloudBackupsModal.open}
        onRequestClose={() => setCloudBackupsModal({ ...cloudBackupsModal, open: false })}
        contentLabel="选择存储桶备份恢复"
        size="lg"
        panelClassName="p-6"
      >
        <div className="flex flex-col items-start w-full">
          <h1 className="text-xl font-bold tracking-[-0.02em] t-primary">从存储桶恢复数据</h1>
          <p className="mt-1 text-sm text-neutral-500">
            当前存储桶（{cloudBackupsModal.target}）历史备份快照：
          </p>

          <div className="mt-4 w-full max-h-80 overflow-y-auto space-y-3">
            {cloudBackupsModal.loading ? (
              <div className="flex items-center justify-center p-8 text-neutral-400 gap-2">
                <ReactLoading type="spin" color="#FC466B" width={20} height={20} />
                <span>正在读取存储桶...</span>
              </div>
            ) : cloudBackupsModal.items.length === 0 ? (
              <div className="p-4 rounded-xl bg-secondary text-sm text-neutral-500 space-y-2">
                <p>存储桶索引暂未获取到，但文件已在存储桶中。请在下方直接输入完整路径恢复。</p>
              </div>
            ) : (
              cloudBackupsModal.items.map((item, idx) => (
                <div
                  key={item.key || idx}
                  className="flex flex-col sm:flex-row sm:items-center justify-between p-4 rounded-xl border border-black/10 dark:border-white/10 bg-secondary gap-3"
                >
                  <div className="min-w-0">
                    <p className="font-mono text-sm font-semibold truncate t-primary">{item.filename}</p>
                    <p className="text-xs text-neutral-400 mt-1">
                      备份时间: {new Date(item.createdAt).toLocaleString()} · 包含文章: {item.metadata?.counts?.feeds ?? 0} 篇
                    </p>
                  </div>
                  <Button
                    title="选择此备份"
                    onClick={() => handleSelectCloudBackupItem(item)}
                  />
                </div>
              ))
            )}
          </div>

          {/* 手动指定云端文件路径恢复兜底 */}
          <div className="mt-5 w-full pt-4 border-t border-black/5 dark:border-white/5 space-y-2">
            <p className="text-xs font-semibold text-neutral-400 uppercase tracking-wider">
              或直接指定存储桶备份文件完整路径恢复：
            </p>
            <div className="flex gap-2">
              <input
                type="text"
                placeholder="例如：backups/rin-backup-2026-10-24T01-43-36.json"
                value={manualCloudKey}
                onChange={(e) => setManualCloudKey(e.target.value)}
                className="w-full rounded-xl border border-black/10 bg-w px-3 py-2 text-xs font-mono t-primary outline-none focus:border-theme dark:border-white/10"
              />
              <Button
                title={backupActionLoading ? "读取中..." : "读取并恢复"}
                disabled={backupActionLoading}
                onClick={async () => {
                  const key = manualCloudKey.trim();
                  if (!key) {
                    showAlert("请输入完整文件路径，例如：backups/rin-backup-2026-10-24T01-43-36.json");
                    return;
                  }
                  setBackupActionLoading(true);
                  try {
                    const { data, error } = await client.backup.inspectStorageBackup(key);
                    if (error || !data || !data.ok) {
                      showAlert(`未能读取到备份文件: ${error?.value || "路径错误或文件不存在"}`);
                    } else {
                      setCloudBackupsModal({ ...cloudBackupsModal, open: false });
                      setRestoreConfirmModal({
                        open: true,
                        source: "cloud",
                        cloudKey: data.key,
                        filename: data.filename,
                        exportedAt: data.exportedAt,
                        counts: data.metadata?.counts || {},
                      });
                    }
                  } catch (e: any) {
                    showAlert(`读取失败: ${e.message}`);
                  } finally {
                    setBackupActionLoading(false);
                  }
                }}
              />
            </div>
            <p className="text-[11px] text-neutral-400">
              *注意：路径中<b>不需要包含存储桶名字</b>，必须带有 <b>.json</b> 后缀。
            </p>
          </div>

          <div className="mt-6 flex w-full justify-end">
            <Button secondary title="关闭" onClick={() => setCloudBackupsModal({ ...cloudBackupsModal, open: false })} />
          </div>
        </div>
      </Modal>

      {/* 4. 恢复前确认弹窗（展示备份内详细内容并提示覆盖警告） */}
      <Modal
        isOpen={Boolean(restoreConfirmModal?.open)}
        onRequestClose={() => setRestoreConfirmModal(null)}
        contentLabel="确认恢复备份"
        panelClassName="p-6"
      >
        {restoreConfirmModal ? (
          <div className="flex flex-col items-start w-full">
            <div className="flex items-center gap-2 text-rose-600 dark:text-rose-400">
              <i className="ri-error-warning-fill text-2xl" />
              <h1 className="text-xl font-bold tracking-[-0.02em] t-primary">确认恢复此份备份？</h1>
            </div>
            <p className="mt-2 text-sm text-rose-600 dark:text-rose-300 font-medium">
              ⚠️ 高风险覆盖警示：恢复将用此备份包彻底覆盖当前站点的文章、动态、评论、友链和自定义配置！此操作无法撤销！
            </p>

            <div className="mt-4 w-full rounded-xl bg-secondary p-4 space-y-2 text-xs sm:text-sm t-primary">
              <p className="font-semibold text-xs uppercase tracking-wider text-theme">即将恢复的数据镜像详情：</p>
              <p>📦 <b>恢复来源：</b> {
                restoreConfirmModal.source === "local"
                  ? "本地上传文件"
                  : restoreConfirmModal.source === "github"
                    ? "GitHub 仓库快照"
                    : "云端存储桶文件"
              }</p>
              <p>🏷️ <b>备份文件名：</b> <span className="font-mono">{restoreConfirmModal.filename}</span></p>
              <p>🕒 <b>备份生成时间：</b> <span>{new Date(restoreConfirmModal.exportedAt).toLocaleString()}</span></p>
              <div className="pt-2 border-t border-black/5 dark:border-white/5 grid grid-cols-2 gap-2 text-xs">
                <span>📄 文章: {restoreConfirmModal.counts?.feeds ?? 0} 篇</span>
                <span>💬 评论: {restoreConfirmModal.counts?.comments ?? 0} 条</span>
                <span>⚡ 动态: {restoreConfirmModal.counts?.moments ?? 0} 条</span>
                <span>🤝 友链: {restoreConfirmModal.counts?.friends ?? 0} 个</span>
              </div>
            </div>

            <div className="mt-6 flex w-full justify-end gap-2">
              <Button secondary title="取消" onClick={() => setRestoreConfirmModal(null)} disabled={backupActionLoading} />
              <Button
                title={backupActionLoading ? "正在恢复写入..." : "确认覆盖并恢复"}
                onClick={executeConfirmedRestore}
                disabled={backupActionLoading}
              />
            </div>
          </div>
        ) : null}
      </Modal>

      {/* WordPress 导入弹窗 */}
      <Modal
        isOpen={isOpen}
        onRequestClose={() => setIsOpen(false)}
        contentLabel={t("settings.import_result")}
        panelClassName="p-6"
      >
        <div className="flex flex-col items-start">
          <h1 className="text-xl font-bold tracking-[-0.02em] t-primary">{t("settings.import_result")}</h1>
          <p className="mt-2 text-base t-primary">{msg}</p>
          <div className="flex w-full flex-col items-start">
            <p className="mt-4 text-base font-bold t-primary">{t("settings.import_skipped")}</p>
            <ul className="flex max-h-64 w-full flex-col items-start overflow-auto">
              {msgList.map((item, idx) => (
                <li key={idx} className="text-sm t-primary">
                  {t("settings.import_skipped_item$title$reason", { title: item.title, reason: item.reason })}
                </li>
              ))}
            </ul>
          </div>
          <div className="mt-4 flex w-full flex-col items-center">
            <button
              onClick={() => {
                setIsOpen(false);
              }}
              className="h-min rounded-xl bg-theme px-8 py-2 text-white"
            >
              {t("close")}
            </button>
          </div>
        </div>
      </Modal>
      <AlertUI />
    </div>
  );
}
```

---

### 四、如何使用 GitHub 备份与恢复（操作说明）

1. **在 GitHub 创建专用私有仓库**：
   - 登录你的 GitHub 账号，点击右上角 `+` -> `New repository`。
   - 仓库名称例如填：`blog-backups`。
   - **⚠️ 务必勾选 `Private`（私有仓库）！**
2. **生成 GitHub Token（Personal Access Token）**：
   - 打开 GitHub `Settings` -> 左下角 `Developer settings` -> `Personal access tokens` -> `Fine-grained tokens`（或者 `Tokens (classic)`） [1]。
   - 新建一个 Token，将权限赋予刚才创建的 `blog-backups` 仓库，给予权限：`Contents: Read and write` [2]。
   - 复制生成的 `ghp_...` 令牌。
3. **在博客后台绑定**：
   - 进入博客后台 **“设置”** -> 点击 **“配置 GitHub 仓库”**。
   - 填入你的 GitHub 用户名、仓库名（如 `blog-backups`）和刚刚复制的 Token，点击“测试连接”确认通过后保存。
4. **开始备份与恢复**：
   - 点击 **“备份到 GitHub 仓库”**：数据将被打包，作为一个标准的 Git Commit 直接提交到你的私有仓库中！
   - 点击 **“从 GitHub 恢复”**：直接调取 GitHub 仓库中的所有提交快照，点击任意一份历史备份即可一键完成完整还原。
Citations

https://github.com/openRin/Rin
https://github.com/kingcun/blog
4m21.4s
Model 15:01
这里为你提供本次改动涉及的全部完整代码，共包含 3 个文件。每个文件均以完整、独立的 Markdown 代码块给出，可直接点击右上角复制并全量覆盖对应文件：

---

### 文件一：`server/src/services/backup.ts`

```typescript
import { inArray } from "drizzle-orm";
import { Hono } from "hono";
import type { AppContext } from "../core/hono-types";
import { adminOnly } from "../core/route-boundaries";
import {
    cache,
    comments,
    feedHashtags,
    feeds,
    friends,
    hashtags,
    moments,
    storageFiles,
    storages,
    users,
    visitStats,
} from "../db/schema";
import { createEnvDriverIfConfigured } from "../utils/storage-drivers";
import { driverForRow, DEFAULT_STORAGE_ID } from "../utils/storage-manager";
import { encryptSecret, decryptSecret } from "../utils/storage-secrets";

const BACKUP_SCHEMA_VERSION = 1;
const MAX_CLOUD_BACKUPS = 3;
const BACKUP_DIR_NAME = "backups";
const CLOUD_BACKUPS_CACHE_KEY = "cloud_backups_list";
const GITHUB_BACKUP_CACHE_KEY = "github_backup_config";

interface StoredGitHubConfig {
    owner: string;
    repo: string;
    branch: string;
    tokenEnc: string;
    path: string;
}

/** 获取并解密 GitHub 备份配置 */
async function getGitHubBackupConfig(env: Env, cacheManager: any): Promise<{
    owner: string;
    repo: string;
    branch: string;
    token: string;
    path: string;
    configured: boolean;
}> {
    const raw = (await cacheManager.get(GITHUB_BACKUP_CACHE_KEY)) as StoredGitHubConfig | null;
    if (!raw || !raw.owner || !raw.repo || !raw.tokenEnc) {
        return { owner: "", repo: "", branch: "main", token: "", path: "backups", configured: false };
    }
    try {
        const token = await decryptSecret(env, raw.tokenEnc);
        return {
            owner: raw.owner.trim(),
            repo: raw.repo.trim(),
            branch: (raw.branch || "main").trim(),
            token,
            path: (raw.path || "backups").trim().replace(/^\/+|\/+$/g, ""),
            configured: Boolean(raw.owner && raw.repo && token),
        };
    } catch {
        return { owner: raw.owner, repo: raw.repo, branch: raw.branch || "main", token: "", path: raw.path || "backups", configured: false };
    }
}

/** 获取当前系统可用的最佳存储驱动并规划备份目录 */
async function getActiveStorageDriver(env: Env, db: any) {
    const customStorages = await db.select().from(storages);
    const active = customStorages.find((s: any) => s.enabled === 1 && s.status === "ok");
    if (active) {
        const driver = await driverForRow(env, active);
        const folder = active.folder ? `${active.folder}/${BACKUP_DIR_NAME}` : BACKUP_DIR_NAME;
        return { driver, folder, name: active.name, bucket: active.bucket, id: active.id };
    }
    const envDriver = createEnvDriverIfConfigured(env);
    if (envDriver) {
        const cleanEnvFolder = envDriver.folder.replace(/images\/?$/, "");
        const folder = cleanEnvFolder ? `${cleanEnvFolder}/${BACKUP_DIR_NAME}` : BACKUP_DIR_NAME;
        return { driver: envDriver.driver, folder, name: "环境变量默认存储", bucket: env.S3_BUCKET || "default", id: DEFAULT_STORAGE_ID };
    }
    return null;
}

/** 智能净化用户输入的云端文件路径，去除误输的桶名或存储名前缀 */
function cleanBackupObjectKey(rawKey: string, bucketName?: string, storageName?: string): string {
    let key = rawKey.trim().replace(/^\/+/, "");
    if (storageName && key.startsWith(`${storageName}/`)) {
        key = key.slice(storageName.length + 1).replace(/^\/+/, "");
    }
    if (bucketName && key.startsWith(`${bucketName}/`)) {
        key = key.slice(bucketName.length + 1).replace(/^\/+/, "");
    }
    return key;
}

export function BackupService(): Hono {
    const app = new Hono();

    // 构建完整数据快照
    async function createSnapshot(db: any) {
        const [
            userRows,
            feedRows,
            momentRows,
            friendRows,
            commentRows,
            hashtagRows,
            feedHashtagRows,
            configRows,
            storageRows,
            storageFileRows,
            visitStatRows,
        ] = await Promise.all([
            db.select().from(users),
            db.select().from(feeds),
            db.select().from(moments),
            db.select().from(friends),
            db.select().from(comments),
            db.select().from(hashtags),
            db.select().from(feedHashtagRows || feedHashtags),
            db.select().from(cache).where(inArray(cache.type, ["client.config", "server.config"])),
            db.select().from(storages),
            db.select().from(storageFiles),
            db.select().from(visitStats),
        ]);

        const totalMediaBytes = storageFileRows.reduce((acc: number, f: any) => acc + (f.size || 0), 0);

        return {
            version: BACKUP_SCHEMA_VERSION,
            app: "rin",
            exportedAt: new Date().toISOString(),
            metadata: {
                counts: {
                    feeds: feedRows.length,
                    moments: momentRows.length,
                    comments: commentRows.length,
                    friends: friendRows.length,
                    hashtags: hashtagRows.length,
                    storages: storageRows.length,
                    mediaFiles: storageFileRows.length,
                },
                mediaSummary: {
                    totalFiles: storageFileRows.length,
                    totalBytes: totalMediaBytes,
                },
            },
            data: {
                users: userRows,
                feeds: feedRows,
                moments: momentRows,
                friends: friendRows,
                comments: commentRows,
                hashtags: hashtagRows,
                feedHashtags: feedHashtagRows,
                configs: configRows,
                storages: storageRows,
                storageFiles: storageFileRows,
                visitStats: visitStatRows,
            },
        };
    }

    // 执行数据恢复核心逻辑（严格防 SQL 变量溢出）
    async function executeRestore(db: any, cacheManager: any, payload: any) {
        if (!payload || !payload.data || payload.app !== "rin") {
            throw new Error("无效的备份文件结构，请确保使用的是 Rin 导出的备份包");
        }

        const data = payload.data;

        // 动态计算列数，确保单次 SQL 变量数保持在 30 个以内，绝不超标
        async function safeInsert<T extends Record<string, any>>(table: any, rows: T[]) {
            if (!rows || rows.length === 0) return;
            const colCount = Math.max(1, Object.keys(rows[0]).length);
            const chunkSize = Math.max(1, Math.floor(30 / colCount));
            for (let i = 0; i < rows.length; i += chunkSize) {
                const chunk = rows.slice(i, i + chunkSize);
                await db.insert(table).values(chunk as any);
            }
        }

        const parseDate = (d: any) => (d ? new Date(d) : new Date());

        // 1. 清理原有数据（按外键依赖逆序安全清理）
        await db.delete(feedHashtags);
        await db.delete(comments);
        await db.delete(visitStats);
        await db.delete(feeds);
        await db.delete(hashtags);
        await db.delete(moments);
        await db.delete(friends);
        await db.delete(storageFiles);
        await db.delete(storages);
        await db.delete(cache).where(inArray(cache.type, ["client.config", "server.config"]));
        await db.delete(users);

        // 2. 依次按依赖关系安全恢复
        if (data.users?.length) {
            await safeInsert(users, data.users.map((u: any) => ({
                ...u,
                createdAt: parseDate(u.createdAt),
                updatedAt: parseDate(u.updatedAt),
            })));
        }

        if (data.hashtags?.length) {
            await safeInsert(hashtags, data.hashtags.map((h: any) => ({
                ...h,
                createdAt: parseDate(h.createdAt),
                updatedAt: parseDate(h.updatedAt),
            })));
        }

        if (data.feeds?.length) {
            await safeInsert(feeds, data.feeds.map((f: any) => ({
                ...f,
                createdAt: parseDate(f.createdAt),
                updatedAt: parseDate(f.updatedAt),
            })));
        }

        if (data.feedHashtags?.length) {
            await safeInsert(feedHashtags, data.feedHashtags.map((fh: any) => ({
                ...fh,
                createdAt: parseDate(fh.createdAt),
                updatedAt: parseDate(fh.updatedAt),
            })));
        }

        if (data.comments?.length) {
            await safeInsert(comments, data.comments.map((cm: any) => ({
                ...cm,
                createdAt: parseDate(cm.createdAt),
                updatedAt: parseDate(cm.updatedAt),
            })));
        }

        if (data.moments?.length) {
            await safeInsert(moments, data.moments.map((m: any) => ({
                ...m,
                createdAt: parseDate(m.createdAt),
                updatedAt: parseDate(m.updatedAt),
            })));
        }

        if (data.friends?.length) {
            await safeInsert(friends, data.friends.map((fr: any) => ({
                ...fr,
                createdAt: parseDate(fr.createdAt),
                updatedAt: parseDate(fr.updatedAt),
            })));
        }

        if (data.storages?.length) {
            await safeInsert(storages, data.storages.map((s: any) => ({
                ...s,
                createdAt: parseDate(s.createdAt),
                updatedAt: parseDate(s.updatedAt),
            })));
        }

        if (data.storageFiles?.length) {
            await safeInsert(storageFiles, data.storageFiles.map((sf: any) => ({
                ...sf,
                createdAt: parseDate(sf.createdAt),
            })));
        }

        if (data.configs?.length) {
            await safeInsert(cache, data.configs.map((cf: any) => ({
                ...cf,
                createdAt: parseDate(cf.createdAt),
                updatedAt: parseDate(cf.updatedAt),
            })));
        }

        if (data.visitStats?.length) {
            await safeInsert(visitStats, data.visitStats.map((vs: any) => ({
                ...vs,
                updatedAt: parseDate(vs.updatedAt),
            })));
        }

        // 清理缓存
        await cacheManager.clear();
    }

    // 0. GET /backup/preview —— 获取即将备份的数据量预览
    app.get("/preview", adminOnly(async (c: AppContext) => {
        const db = c.get("db");
        const env = c.get("env");
        const cacheManager = c.get("cache");

        const [feedCount, commentCount, momentCount, friendCount, storageCount] = await Promise.all([
            db.select().from(feeds),
            db.select().from(comments),
            db.select().from(moments),
            db.select().from(friends),
            db.select().from(storages),
        ]);

        const target = await getActiveStorageDriver(env, db);
        const gh = await getGitHubBackupConfig(env, cacheManager);

        return c.json({
            ok: true,
            preview: {
                feedsCount: feedCount.length,
                commentsCount: commentCount.length,
                momentsCount: momentCount.length,
                friendsCount: friendCount.length,
                storagesCount: storageCount.length,
            },
            targetStorage: {
                configured: Boolean(target),
                name: target?.name || "未配置",
                folder: target ? target.folder : "",
            },
            targetGitHub: {
                configured: gh.configured,
                repo: gh.configured ? `${gh.owner}/${gh.repo}` : "未配置",
                branch: gh.branch,
                path: gh.path,
            },
        });
    }));

    // 1. GET /backup/export —— 下载本地备份 JSON 文件
    app.get("/export", adminOnly(async (c: AppContext) => {
        const db = c.get("db");
        const snapshot = await createSnapshot(db);
        const jsonStr = JSON.stringify(snapshot, null, 2);
        const dateStr = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
        const fileName = `rin-backup-${dateStr}.json`;

        return new Response(jsonStr, {
            status: 200,
            headers: {
                "Content-Type": "application/json; charset=utf-8",
                "Content-Disposition": `attachment; filename="${fileName}"`,
                "Cache-Control": "no-store",
            },
        });
    }));

    // 2. POST /backup/export-to-storage —— 备份并存入存储桶
    app.post("/export-to-storage", adminOnly(async (c: AppContext) => {
        const db = c.get("db");
        const env = c.get("env");
        const cacheManager = c.get("cache");

        const target = await getActiveStorageDriver(env, db);
        if (!target) {
            return c.json({ ok: false, error: "未找到任何可用的存储桶，请先在「存储管理」绑定一个存储。" }, 400);
        }

        const snapshot = await createSnapshot(db);
        const jsonStr = JSON.stringify(snapshot, null, 2);
        const dateStr = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
        const fileName = `rin-backup-${dateStr}.json`;
        const objectKey = `${target.folder}/${fileName}`.replace(/\/+/g, "/");
        const indexKey = `${target.folder}/index.json`.replace(/\/+/g, "/");

        try {
            await target.driver.put(objectKey, jsonStr, "application/json");

            let backupList: any[] = [];
            try {
                const indexRes = await target.driver.get(indexKey);
                if (indexRes && indexRes.ok) {
                    const text = await indexRes.text();
                    backupList = JSON.parse(text);
                    if (!Array.isArray(backupList)) backupList = [];
                }
            } catch {
                backupList = [];
            }

            const newEntry = {
                key: objectKey,
                filename: fileName,
                createdAt: snapshot.exportedAt,
                metadata: snapshot.metadata,
            };

            backupList = backupList.filter((b) => b.key !== objectKey);
            backupList.unshift(newEntry);

            let prunedCount = 0;
            if (backupList.length > MAX_CLOUD_BACKUPS) {
                const toPrune = backupList.slice(MAX_CLOUD_BACKUPS);
                backupList = backupList.slice(0, MAX_CLOUD_BACKUPS);
                for (const old of toPrune) {
                    if (old.key) {
                        await target.driver.delete(old.key).catch(() => {});
                        prunedCount++;
                    }
                }
            }

            await target.driver.put(indexKey, JSON.stringify(backupList, null, 2), "application/json");
            await cacheManager.set(CLOUD_BACKUPS_CACHE_KEY, backupList).catch(() => {});

            return c.json({
                ok: true,
                filename: fileName,
                target: target.name,
                path: objectKey,
                exportedAt: snapshot.exportedAt,
                metadata: snapshot.metadata,
                totalBackups: backupList.length,
                prunedCount,
            });
        } catch (error: any) {
            return c.json({ ok: false, error: `写入存储桶失败: ${error.message || String(error)}` }, 500);
        }
    }));

    // 3. GET /backup/storage-list —— 获取存储桶备份列表
    app.get("/storage-list", adminOnly(async (c: AppContext) => {
        const db = c.get("db");
        const env = c.get("env");
        const cacheManager = c.get("cache");

        const target = await getActiveStorageDriver(env, db);
        if (!target) {
            return c.json({ ok: false, error: "未配置存储桶" }, 400);
        }

        const indexKey = `${target.folder}/index.json`.replace(/\/+/g, "/");
        let items: any[] = [];
        let readError: string | null = null;
        try {
            const indexRes = await target.driver.get(indexKey);
            if (indexRes && indexRes.ok) {
                const text = await indexRes.text();
                items = JSON.parse(text);
                if (!Array.isArray(items)) items = [];
            }
        } catch (err: any) {
            readError = err?.message || String(err);
            console.error("[backup] storage-list read error:", err);
        }

        // 双重保障回退：若云端 index.json 未拿到，自动从本地数据库缓存读取
        if (items.length === 0) {
            try {
                const cached = await cacheManager.get(CLOUD_BACKUPS_CACHE_KEY);
                if (Array.isArray(cached) && cached.length > 0) {
                    items = cached;
                }
            } catch {}
        }

        return c.json({
            ok: true,
            target: target.name,
            items,
            error: items.length === 0 ? readError : undefined,
        });
    }));

    // 4. POST /backup/inspect-storage-backup —— 检查存储桶指定路径的备份文件
    app.post("/inspect-storage-backup", adminOnly(async (c: AppContext) => {
        const db = c.get("db");
        const env = c.get("env");
        const body = (await c.req.json().catch(() => ({}))) as { key?: string };
        const rawKey = body.key?.trim();
        if (!rawKey) {
            return c.json({ ok: false, error: "请输入备份文件完整路径" }, 400);
        }

        const target = await getActiveStorageDriver(env, db);
        if (!target) {
            return c.json({ ok: false, error: "未配置存储桶" }, 400);
        }

        const key = cleanBackupObjectKey(rawKey, target.bucket, target.name);

        if (!key.endsWith(".json")) {
            return c.json({
                ok: false,
                error: `请输入以 .json 结尾的完整文件名，例如：backups/rin-backup-2026-10-24T01-43-36.json（您当前输入的是: ${rawKey}）`,
            }, 400);
        }

        try {
            const fileRes = await target.driver.get(key);
            if (!fileRes || !fileRes.ok) {
                return c.json({ ok: false, error: `在存储桶中未找到文件: ${key}（请确认路径与文件名拼写正确）` }, 404);
            }
            const payload = JSON.parse(await fileRes.text());
            if (!payload || payload.app !== "rin" || !payload.data) {
                return c.json({ ok: false, error: "该文件不是有效的 Rin 备份快照" }, 400);
            }

            return c.json({
                ok: true,
                key,
                filename: key.split("/").pop() || key,
                exportedAt: payload.exportedAt || "未知",
                metadata: payload.metadata || {},
            });
        } catch (error: any) {
            return c.json({ ok: false, error: error.message || "读取备份文件失败" }, 500);
        }
    }));

    // 5. POST /backup/restore-from-storage —— 从存储桶恢复
    app.post("/restore-from-storage", adminOnly(async (c: AppContext) => {
        const db = c.get("db");
        const env = c.get("env");
        const cacheManager = c.get("cache");

        const body = (await c.req.json().catch(() => ({}))) as { key?: string };
        const rawKey = body.key?.trim();
        if (!rawKey) {
            return c.json({ ok: false, error: "缺少备份文件路径" }, 400);
        }

        const target = await getActiveStorageDriver(env, db);
        if (!target) {
            return c.json({ ok: false, error: "存储驱动未就绪" }, 400);
        }

        const key = cleanBackupObjectKey(rawKey, target.bucket, target.name);

        try {
            const fileRes = await target.driver.get(key);
            if (!fileRes || !fileRes.ok) {
                return c.json({ ok: false, error: `无法从存储桶读取指定的备份文件: ${key}` }, 404);
            }

            const payload = JSON.parse(await fileRes.text());
            await executeRestore(db, cacheManager, payload);

            return c.json({
                ok: true,
                message: "从存储桶备份恢复成功！",
                counts: payload.metadata?.counts || {},
            });
        } catch (error: any) {
            return c.json({ ok: false, error: error.message || "恢复失败" }, 500);
        }
    }));

    // 6. POST /backup/import —— 从上传的本地文件恢复
    app.post("/import", adminOnly(async (c: AppContext) => {
        const db = c.get("db");
        const cacheManager = c.get("cache");
        let payload: any;

        const contentType = c.req.header("content-type") || "";
        if (contentType.includes("multipart/form-data")) {
            const form = await c.req.parseBody();
            const file = form.file as File | string | undefined;
            if (!file || typeof file === "string") {
                return c.json({ ok: false, error: "请上传备份文件（.json）" }, 400);
            }
            try {
                payload = JSON.parse(await file.text());
            } catch {
                return c.json({ ok: false, error: "备份文件损坏，无法解析为 JSON" }, 400);
            }
        } else {
            payload = await c.req.json().catch(() => null);
        }

        try {
            await executeRestore(db, cacheManager, payload);
            return c.json({
                ok: true,
                message: "数据恢复成功！",
                counts: payload.metadata?.counts || {},
            });
        } catch (error: any) {
            return c.json({ ok: false, error: error.message || "恢复失败" }, 500);
        }
    }));

    // =========================================================================
    // ★ GitHub 专属备份与恢复 API
    // =========================================================================

    // 7. GET /backup/github-config —— 获取 GitHub 备份配置
    app.get("/github-config", adminOnly(async (c: AppContext) => {
        const env = c.get("env");
        const cacheManager = c.get("cache");
        const cfg = await getGitHubBackupConfig(env, cacheManager);

        return c.json({
            ok: true,
            configured: cfg.configured,
            owner: cfg.owner,
            repo: cfg.repo,
            branch: cfg.branch,
            path: cfg.path,
            hasToken: Boolean(cfg.token),
        });
    }));

    // 8. POST /backup/github-config —— 保存 GitHub 备份配置
    app.post("/github-config", adminOnly(async (c: AppContext) => {
        const env = c.get("env");
        const cacheManager = c.get("cache");

        const body = (await c.req.json().catch(() => ({}))) as {
            owner?: string;
            repo?: string;
            branch?: string;
            path?: string;
            token?: string;
        };

        const existing = (await cacheManager.get(GITHUB_BACKUP_CACHE_KEY)) as StoredGitHubConfig | null;

        const owner = (body.owner !== undefined ? body.owner : existing?.owner || "").trim();
        const repo = (body.repo !== undefined ? body.repo : existing?.repo || "").trim();
        const branch = (body.branch !== undefined ? body.branch : existing?.branch || "main").trim() || "main";
        const path = (body.path !== undefined ? body.path : existing?.path || "backups").trim().replace(/^\/+|\/+$/g, "");

        let tokenEnc = existing?.tokenEnc || "";
        if (body.token?.trim()) {
            tokenEnc = await encryptSecret(env, body.token.trim());
        }

        if (!owner || !repo || !tokenEnc) {
            return c.json({ ok: false, error: "请完整填写仓库拥有者 (Owner)、仓库名 (Repo) 以及 GitHub Personal Access Token" }, 400);
        }

        const toSave: StoredGitHubConfig = {
            owner,
            repo,
            branch,
            path: path || "backups",
            tokenEnc,
        };

        await cacheManager.set(GITHUB_BACKUP_CACHE_KEY, toSave);

        return c.json({
            ok: true,
            message: "GitHub 备份配置保存成功",
            configured: true,
            owner,
            repo,
            branch,
            path: toSave.path,
            hasToken: true,
        });
    }));

    // 9. POST /backup/github-test —— 测试 GitHub 仓库连接与权限
    app.post("/github-test", adminOnly(async (c: AppContext) => {
        const env = c.get("env");
        const cacheManager = c.get("cache");
        const cfg = await getGitHubBackupConfig(env, cacheManager);

        if (!cfg.configured) {
            return c.json({ ok: false, error: "GitHub 备份尚未配置完成" }, 400);
        }

        try {
            const res = await fetch(`https://api.github.com/repos/${cfg.owner}/${cfg.repo}`, {
                method: "GET",
                headers: {
                    Authorization: `Bearer ${cfg.token}`,
                    Accept: "application/vnd.github+json",
                    "User-Agent": "Rin-Blog-Backup/1.0",
                },
            });

            if (!res.ok) {
                const text = await res.text();
                return c.json({ ok: false, error: `GitHub API 返回错误 (${res.status}): ${text}` }, 400);
            }

            const repoData: any = await res.json();
            return c.json({
                ok: true,
                isPrivate: repoData.private === true,
                defaultBranch: repoData.default_branch,
                message: repoData.private ? "连接成功！目标为私有仓库，符合安全标准。" : "连接成功！⚠️ 警告：当前目标仓库为公开仓库（Public），为保护隐私请强烈建议在 GitHub 设为 Private！",
            });
        } catch (e: any) {
            return c.json({ ok: false, error: `请求 GitHub 失败: ${e.message}` }, 500);
        }
    }));

    // 10. POST /backup/export-to-github —— 导出全量快照并自动 Commit 到 GitHub 仓库
    app.post("/export-to-github", adminOnly(async (c: AppContext) => {
        const db = c.get("db");
        const env = c.get("env");
        const cacheManager = c.get("cache");

        const cfg = await getGitHubBackupConfig(env, cacheManager);
        if (!cfg.configured) {
            return c.json({ ok: false, error: "请先配置并保存 GitHub 备份仓库信息" }, 400);
        }

        const snapshot = await createSnapshot(db);
        const jsonStr = JSON.stringify(snapshot, null, 2);
        const dateStr = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
        const fileName = `rin-backup-${dateStr}.json`;
        const filePath = cfg.path ? `${cfg.path}/${fileName}` : fileName;

        // Base64 编码文件内容（UTF-8 安全）
        const bytes = new TextEncoder().encode(jsonStr);
        let binary = "";
        for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
        const contentBase64 = btoa(binary);

        const commitMessage = `chore: auto backup ${fileName} [feeds:${snapshot.metadata.counts.feeds}, moments:${snapshot.metadata.counts.moments}]`;

        try {
            const putRes = await fetch(`https://api.github.com/repos/${cfg.owner}/${cfg.repo}/contents/${filePath}`, {
                method: "PUT",
                headers: {
                    Authorization: `Bearer ${cfg.token}`,
                    Accept: "application/vnd.github+json",
                    "User-Agent": "Rin-Blog-Backup/1.0",
                    "Content-Type": "application/json",
                },
                body: JSON.stringify({
                    message: commitMessage,
                    content: contentBase64,
                    branch: cfg.branch,
                }),
            });

            if (!putRes.ok) {
                const text = await putRes.text();
                return c.json({ ok: false, error: `GitHub 写入失败 (${putRes.status}): ${text}` }, 400);
            }

            const putData: any = await putRes.json();

            return c.json({
                ok: true,
                filename: fileName,
                path: filePath,
                target: `GitHub (${cfg.owner}/${cfg.repo})`,
                commitSha: putData.commit?.sha?.slice(0, 7),
                htmlUrl: putData.content?.html_url,
                exportedAt: snapshot.exportedAt,
                metadata: snapshot.metadata,
            });
        } catch (e: any) {
            return c.json({ ok: false, error: `提交到 GitHub 异常: ${e.message}` }, 500);
        }
    }));

    // 11. GET /backup/github-list —— 从 GitHub 仓库列出所有历史备份快照
    app.get("/github-list", adminOnly(async (c: AppContext) => {
        const env = c.get("env");
        const cacheManager = c.get("cache");
        const cfg = await getGitHubBackupConfig(env, cacheManager);

        if (!cfg.configured) {
            return c.json({ ok: false, error: "未配置 GitHub 备份仓库" }, 400);
        }

        try {
            const url = `https://api.github.com/repos/${cfg.owner}/${cfg.repo}/contents/${cfg.path}?ref=${cfg.branch}`;
            const res = await fetch(url, {
                method: "GET",
                headers: {
                    Authorization: `Bearer ${cfg.token}`,
                    Accept: "application/vnd.github+json",
                    "User-Agent": "Rin-Blog-Backup/1.0",
                },
            });

            if (res.status === 404) {
                return c.json({
                    ok: true,
                    target: `${cfg.owner}/${cfg.repo}`,
                    items: [],
                });
            }

            if (!res.ok) {
                const text = await res.text();
                return c.json({ ok: false, error: `读取 GitHub 失败 (${res.status}): ${text}` }, 400);
            }

            const files: any[] = await res.json();
            if (!Array.isArray(files)) {
                return c.json({ ok: true, target: `${cfg.owner}/${cfg.repo}`, items: [] });
            }

            const backupFiles = files
                .filter((f) => f.type === "file" && f.name.startsWith("rin-backup-") && f.name.endsWith(".json"))
                .map((f) => ({
                    filename: f.name,
                    path: f.path,
                    downloadUrl: f.download_url,
                    size: f.size,
                    sha: f.sha,
                    createdAt: extractDateFromBackupName(f.name),
                }))
                .sort((a, b) => b.filename.localeCompare(a.filename));

            return c.json({
                ok: true,
                target: `${cfg.owner}/${cfg.repo}`,
                branch: cfg.branch,
                items: backupFiles,
            });
        } catch (e: any) {
            return c.json({ ok: false, error: `获取 GitHub 列表失败: ${e.message}` }, 500);
        }
    }));

    // 12. POST /backup/inspect-github-backup —— 预览指定 GitHub 备份
    app.post("/inspect-github-backup", adminOnly(async (c: AppContext) => {
        const env = c.get("env");
        const cacheManager = c.get("cache");
        const cfg = await getGitHubBackupConfig(env, cacheManager);

        if (!cfg.configured) {
            return c.json({ ok: false, error: "未配置 GitHub 备份仓库" }, 400);
        }

        const body = (await c.req.json().catch(() => ({}))) as { path?: string };
        const filePath = body.path?.trim();
        if (!filePath) {
            return c.json({ ok: false, error: "缺少文件路径" }, 400);
        }

        try {
            const res = await fetch(`https://api.github.com/repos/${cfg.owner}/${cfg.repo}/contents/${filePath}?ref=${cfg.branch}`, {
                method: "GET",
                headers: {
                    Authorization: `Bearer ${cfg.token}`,
                    Accept: "application/vnd.github.raw+json",
                    "User-Agent": "Rin-Blog-Backup/1.0",
                },
            });

            if (!res.ok) {
                return c.json({ ok: false, error: `无法从 GitHub 仓库读取文件 (${res.status})` }, 404);
            }

            const payload: any = await res.json();
            if (!payload || payload.app !== "rin" || !payload.data) {
                return c.json({ ok: false, error: "该文件不是有效的 Rin 备份文件" }, 400);
            }

            return c.json({
                ok: true,
                path: filePath,
                filename: filePath.split("/").pop() || filePath,
                exportedAt: payload.exportedAt || "未知",
                metadata: payload.metadata || {},
            });
        } catch (e: any) {
            return c.json({ ok: false, error: `解析 GitHub 文件失败: ${e.message}` }, 500);
        }
    }));

    // 13. POST /backup/restore-from-github —— 从 GitHub 仓库恢复
    app.post("/restore-from-github", adminOnly(async (c: AppContext) => {
        const db = c.get("db");
        const env = c.get("env");
        const cacheManager = c.get("cache");
        const cfg = await getGitHubBackupConfig(env, cacheManager);

        if (!cfg.configured) {
            return c.json({ ok: false, error: "未配置 GitHub 备份仓库" }, 400);
        }

        const body = (await c.req.json().catch(() => ({}))) as { path?: string };
        const filePath = body.path?.trim();
        if (!filePath) {
            return c.json({ ok: false, error: "缺少备份文件路径" }, 400);
        }

        try {
            const res = await fetch(`https://api.github.com/repos/${cfg.owner}/${cfg.repo}/contents/${filePath}?ref=${cfg.branch}`, {
                method: "GET",
                headers: {
                    Authorization: `Bearer ${cfg.token}`,
                    Accept: "application/vnd.github.raw+json",
                    "User-Agent": "Rin-Blog-Backup/1.0",
                },
            });

            if (!res.ok) {
                return c.json({ ok: false, error: `无法下载指定的 GitHub 备份文件 (${res.status})` }, 404);
            }

            const payload: any = await res.json();
            await executeRestore(db, cacheManager, payload);

            return c.json({
                ok: true,
                message: "从 GitHub 备份恢复成功！",
                counts: payload.metadata?.counts || {},
            });
        } catch (e: any) {
            return c.json({ ok: false, error: `恢复失败: ${e.message}` }, 500);
        }
    }));

    return app;
}

function extractDateFromBackupName(name: string): string {
    const match = /rin-backup-([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}-[0-9]{2}-[0-9]{2})/.exec(name);
    if (!match) return "未知时间";
    return match[1].replace(/T/, " ").replace(/-/g, (m, offset) => (offset > 10 ? ":" : "-"));
}
```

---

### 文件二：`client/src/api/client.ts`

```typescript
// API Client for Rin - Type-safe HTTP client to replace Eden
// This client provides a clean, type-safe interface for all backend API endpoints

import { getAuthToken } from "../utils/auth";
import { endpoint } from "../config";

// Import shared types
import type {
  ApiResponse,
  RequestOptions,
  Feed,
  FeedListResponse,
  TimelineItem,
  CreateFeedRequest,
  UpdateFeedRequest,
  AdjacentFeedResponse,
  UserProfile,
  UpdateProfileRequest,
  Tag,
  TagDetail,
  Comment,
  CreateCommentRequest,
  Friend,
  FriendListResponse,
  CreateFriendRequest,
  UpdateFriendRequest,
  Moment,
  CreateMomentRequest,
  ConfigType,
  ConfigResponse,
  AIConfig,
  UploadResponse,
  StorageItem,
  StorageInput,
  StorageListResponse,
  StorageTestResponse,
  AuthStatus,
  LoginRequest,
  LoginResponse,
} from "@rin/api";

export interface SettingsConfigResponse {
  clientConfig: ConfigResponse;
  serverConfig: ConfigResponse;
}

export interface ConfigHealthItem {
  id: string;
  title: {
    key: string;
    values?: Record<string, string | number | boolean>;
  };
  status: "success" | "warning" | "danger";
  configured: boolean;
  impact: {
    key: string;
    values?: Record<string, string | number | boolean>;
  };
  summary: {
    key: string;
    values?: Record<string, string | number | boolean>;
  };
  suggestion?: {
    key: string;
    values?: Record<string, string | number | boolean>;
  };
  details?: Array<{
    key: string;
    values?: Record<string, string | number | boolean>;
  }>;
}

export interface ConfigHealthResponse {
  generatedAt: string;
  summary: Record<"success" | "warning" | "danger", number>;
  items: ConfigHealthItem[];
}

export interface QueueStatusItem {
  id: number;
  title: string | null;
  aiSummaryStatus: "idle" | "pending" | "processing" | "completed" | "failed";
  aiSummaryError: string;
  updatedAt: string;
  createdAt: string;
}

export interface QueueStatusResponse {
  queueConfigured: boolean;
  generatedAt: string;
  summary: Record<"idle" | "pending" | "processing" | "completed" | "failed", number>;
  items: QueueStatusItem[];
}

export interface QueueTaskActionResponse {
  success: boolean;
}

export interface CompatTasksResponse {
  generatedAt: string;
  aiSummary: {
    enabled: boolean;
    queueConfigured: boolean;
    eligible: number;
    forceEligible: number;
  };
  blurhash: {
    eligible: number;
  };
}

export interface CompatAISummaryActionResponse {
  queued: number;
  skipped: number;
  forced: boolean;
}

export interface CompatBlurhashCandidate {
  id: number;
  title: string | null;
  content: string;
}

export interface CompatBlurhashCandidatesResponse {
  generatedAt: string;
  items: CompatBlurhashCandidate[];
}

export interface CompatBlurhashApplyResponse {
  updated: boolean;
}

// 备份相关数据结构
export interface BackupPreviewResponse {
  ok: boolean;
  preview: {
    feedsCount: number;
    commentsCount: number;
    momentsCount: number;
    friendsCount: number;
    storagesCount: number;
  };
  targetStorage: {
    configured: boolean;
    name: string;
    folder: string;
  };
  targetGitHub: {
    configured: boolean;
    repo: string;
    branch: string;
    path: string;
  };
}

export interface BackupExportToStorageResponse {
  ok: boolean;
  filename: string;
  path: string;
  target: string;
  metadata: any;
  totalBackups: number;
  prunedCount: number;
}

export interface BackupStorageListResponse {
  ok: boolean;
  target: string;
  items: Array<{
    key: string;
    filename: string;
    createdAt: string;
    metadata?: any;
  }>;
  error?: string;
}

export interface BackupInspectResponse {
  ok: boolean;
  key: string;
  filename: string;
  exportedAt: string;
  metadata: any;
}

export interface BackupRestoreResponse {
  ok: boolean;
  message: string;
  counts: any;
}

// GitHub 备份配置与列表类型
export interface GitHubBackupConfigResponse {
  ok: boolean;
  configured: boolean;
  owner: string;
  repo: string;
  branch: string;
  path: string;
  hasToken: boolean;
}

export interface GitHubBackupItem {
  filename: string;
  path: string;
  downloadUrl: string;
  size: number;
  sha: string;
  createdAt: string;
}

export interface GitHubBackupListResponse {
  ok: boolean;
  target: string;
  branch: string;
  items: GitHubBackupItem[];
  error?: string;
}

// Re-export for external use
export type {
  ApiResponse,
  RequestOptions,
  Feed,
  FeedListResponse,
  TimelineItem,
  CreateFeedRequest,
  UpdateFeedRequest,
  AdjacentFeedResponse,
  UserProfile,
  UpdateProfileRequest,
  Tag,
  TagDetail,
  Comment,
  CreateCommentRequest,
  Friend,
  FriendListResponse,
  CreateFriendRequest,
  UpdateFriendRequest,
  Moment,
  CreateMomentRequest,
  ConfigType,
  ConfigResponse,
  AIConfig,
  UploadResponse,
  StorageItem,
  StorageInput,
  StorageListResponse,
  StorageTestResponse,
  AuthStatus,
  LoginRequest,
  LoginResponse,
} from "@rin/api";

/**
 * HTTP client for making API requests
 */
class HttpClient {
  constructor(private baseUrl: string) {}

  private async request<T>(
    method: string,
    path: string,
    body?: unknown,
    options?: RequestOptions
  ): Promise<ApiResponse<T>> {
    const url = `${this.baseUrl}${path}`;
    const headers: Record<string, string> = {
      Accept: "application/json",
      ...options?.headers,
    };

    // Add auth token if available
    const token = getAuthToken();
    if (token) {
      headers["Authorization"] = `Bearer ${token}`;
    }

    if (body !== undefined && !(body instanceof FormData)) {
      headers["Content-Type"] = "application/json";
    }

    try {
      const response = await fetch(url, {
        method,
        headers,
        body: body instanceof FormData ? body : body ? JSON.stringify(body) : undefined,
        credentials: "include",
      });

      // Handle 204 No Content
      if (response.status === 204) {
        return { data: undefined as T };
      }

      if (!response.ok) {
        let errorValue: unknown;
        const responseClone = response.clone();
        try {
          errorValue = await response.json();
        } catch {
          errorValue = await responseClone.text();
        }
        let errorMessage: string;
        if (typeof errorValue === 'string') {
          errorMessage = errorValue;
        } else if (errorValue && typeof errorValue === 'object') {
          const err = errorValue as any;
          errorMessage = err.error?.message || err.message || err.error || JSON.stringify(errorValue);
        } else {
          errorMessage = String(errorValue ?? response.statusText);
        }
        return {
          error: {
            status: response.status,
            value: errorMessage,
          },
        };
      }

      const contentLength = response.headers.get("content-length");
      if (contentLength === "0") {
        return { data: undefined as T };
      }

      const contentType = response.headers.get("content-type");
      if (contentType?.includes("application/json")) {
        const data = await response.json();
        return { data };
      }

      const text = await response.text();
      return { data: text as T };
    } catch (error) {
      return {
        error: {
          status: 0,
          value: error instanceof Error ? error.message : "Network error",
        },
      };
    }
  }

  async get<T>(path: string, options?: RequestOptions): Promise<ApiResponse<T>> {
    return this.request<T>("GET", path, undefined, options);
  }

  async post<T>(path: string, body?: unknown, options?: RequestOptions): Promise<ApiResponse<T>> {
    return this.request<T>("POST", path, body, options);
  }

  async put<T>(path: string, body?: unknown, options?: RequestOptions): Promise<ApiResponse<T>> {
    return this.request<T>("PUT", path, body, options);
  }

  async patch<T>(path: string, body?: unknown, options?: RequestOptions): Promise<ApiResponse<T>> {
    return this.request<T>("PATCH", path, body, options);
  }

  async delete<T>(path: string, options?: RequestOptions): Promise<ApiResponse<T>> {
    return this.request<T>("DELETE", path, undefined, options);
  }
}

/**
 * Feed API methods
 */
class FeedAPI {
  constructor(private http: HttpClient) {}

  async list(params?: { page?: number; limit?: number; type?: 'draft' | 'unlisted' | 'normal' }): Promise<ApiResponse<FeedListResponse>> {
    const searchParams = new URLSearchParams();
    if (params?.page) searchParams.set("page", params.page.toString());
    if (params?.limit) searchParams.set("limit", params.limit.toString());
    if (params?.type) searchParams.set("type", params.type);
    
    const query = searchParams.toString();
    return this.http.get<FeedListResponse>(`/api/feed${query ? `?${query}` : ""}`);
  }

  async timeline(): Promise<ApiResponse<TimelineItem[]>> {
    return this.http.get<TimelineItem[]>("/api/feed/timeline");
  }

  async get(id: number | string): Promise<ApiResponse<Feed>> {
    return this.http.get<Feed>(`/api/feed/${id}`);
  }

  async create(body: CreateFeedRequest): Promise<ApiResponse<{ insertedId: number }>> {
    return this.http.post<{ insertedId: number }>("/api/feed", body);
  }

  async update(id: number, body: UpdateFeedRequest): Promise<ApiResponse<void>> {
    return this.http.post<void>(`/api/feed/${id}`, body);
  }

  async delete(id: number): Promise<ApiResponse<void>> {
    return this.http.delete<void>(`/api/feed/${id}`);
  }

  async adjacent(id: number | string): Promise<ApiResponse<AdjacentFeedResponse>> {
    return this.http.get<AdjacentFeedResponse>(`/api/feed/adjacent/${id}`);
  }

  async setTop(id: number, top: number): Promise<ApiResponse<void>> {
    return this.http.post<void>(`/api/feed/top/${id}`, { top });
  }
}

/**
 * Tag API methods
 */
class TagAPI {
  constructor(private http: HttpClient) {}

  async list(): Promise<ApiResponse<Tag[]>> {
    return this.http.get<Tag[]>("/api/tag");
  }

  async get(name: string): Promise<ApiResponse<TagDetail>> {
    return this.http.get<TagDetail>(`/api/tag/${encodeURIComponent(name)}`);
  }
}

/**
 * Comment API methods
 */
class CommentAPI {
  constructor(private http: HttpClient) {}

  async list(feedId: number): Promise<ApiResponse<Comment[]>> {
    return this.http.get<Comment[]>(`/api/comment/${feedId}`);
  }

  async create(feedId: number, body: CreateCommentRequest): Promise<ApiResponse<Comment>> {
    return this.http.post<Comment>(`/api/comment/${feedId}`, body);
  }

  async delete(id: number): Promise<ApiResponse<void>> {
    return this.http.delete<void>(`/api/comment/${id}`);
  }
}

/**
 * User API methods
 */
class UserAPI {
  constructor(private http: HttpClient) {}

  async profile(): Promise<ApiResponse<UserProfile>> {
    return this.http.get<UserProfile>("/api/user/profile");
  }

  async updateProfile(body: UpdateProfileRequest): Promise<ApiResponse<{ success: boolean }>> {
    return this.http.put<{ success: boolean }>("/api/user/profile", body);
  }

  async logout(): Promise<ApiResponse<void>> {
    return this.http.post<void>("/api/user/logout");
  }

  githubAuth(): string {
    return `${endpoint}/api/user/github`;
  }
}

/**
 * Friend API methods
 */
class FriendAPI {
  constructor(private http: HttpClient) {}

  async list(): Promise<ApiResponse<FriendListResponse>> {
    return this.http.get<FriendListResponse>("/api/friend");
  }

  async create(body: CreateFriendRequest): Promise<ApiResponse<Friend>> {
    return this.http.post<Friend>(`/api/friend`, body);
  }

  async update(id: number, body: UpdateFriendRequest): Promise<ApiResponse<Friend>> {
    return this.http.put<Friend>(`/api/friend/${id}`, body);
  }

  async delete(id: number): Promise<ApiResponse<void>> {
    return this.http.delete<void>(`/api/friend/${id}`);
  }
}

/**
 * Moments API methods
 */
class MomentsAPI {
  constructor(private http: HttpClient) {}

  async list(params?: { page?: number; limit?: number }): Promise<ApiResponse<{ data: Moment[]; hasNext: boolean }>> {
    const searchParams = new URLSearchParams();
    if (params?.page) searchParams.set("page", params.page.toString());
    if (params?.limit) searchParams.set("limit", params.limit.toString());
    
    const query = searchParams.toString();
    return this.http.get<{ data: Moment[]; hasNext: boolean }>(`/api/moments${query ? `?${query}` : ""}`);
  }

  async create(body: CreateMomentRequest): Promise<ApiResponse<Moment>> {
    return this.http.post<Moment>("/api/moments", body);
  }

  async update(id: number, body: CreateMomentRequest): Promise<ApiResponse<Moment>> {
    return this.http.post<Moment>(`/api/moments/${id}`, body);
  }

  async delete(id: number): Promise<ApiResponse<void>> {
    return this.http.delete<void>(`/api/moments/${id}`);
  }
}

/**
 * Config API methods
 */
class ConfigAPI {
  constructor(private http: HttpClient) {}

  async getAll(): Promise<ApiResponse<SettingsConfigResponse>> {
    return this.http.get<SettingsConfigResponse>("/api/config");
  }

  async get(type: ConfigType): Promise<ApiResponse<ConfigResponse>> {
    return this.http.get<ConfigResponse>(`/api/config/${type}`);
  }

  async updateAll(body: SettingsConfigResponse): Promise<ApiResponse<SettingsConfigResponse>> {
    return this.http.post<SettingsConfigResponse>("/api/config", body);
  }

  async update(type: ConfigType, body: Record<string, unknown>): Promise<ApiResponse<void>> {
    return this.http.post<void>(`/api/config/${type}`, body);
  }

  async clearCache(): Promise<ApiResponse<void>> {
    return this.http.delete<void>("/api/config/cache");
  }

  async getHealth(): Promise<ApiResponse<ConfigHealthResponse>> {
    return this.http.get<ConfigHealthResponse>("/api/config/health");
  }

  async getQueueStatus(): Promise<ApiResponse<QueueStatusResponse>> {
    return this.http.get<QueueStatusResponse>("/api/config/queue-status");
  }

  async getCompatTasks(): Promise<ApiResponse<CompatTasksResponse>> {
    return this.http.get<CompatTasksResponse>("/api/config/compat-tasks");
  }

  async runCompatAISummary(force = false): Promise<ApiResponse<CompatAISummaryActionResponse>> {
    return this.http.post<CompatAISummaryActionResponse>("/api/config/compat-tasks/ai-summary", { force });
  }

  async getCompatBlurhashCandidates(): Promise<ApiResponse<CompatBlurhashCandidatesResponse>> {
    return this.http.get<CompatBlurhashCandidatesResponse>("/api/config/compat-tasks/blurhash");
  }

  async applyCompatBlurhash(feedId: number, content: string): Promise<ApiResponse<CompatBlurhashApplyResponse>> {
    return this.http.post<CompatBlurhashApplyResponse>(`/api/config/compat-tasks/blurhash/${feedId}`, { content });
  }

  async retryQueueTask(feedId: number): Promise<ApiResponse<QueueTaskActionResponse>> {
    return this.http.post<QueueTaskActionResponse>(`/api/config/queue-status/${feedId}/retry`);
  }

  async deleteQueueTask(feedId: number): Promise<ApiResponse<QueueTaskActionResponse>> {
    return this.http.delete<QueueTaskActionResponse>(`/api/config/queue-status/${feedId}`);
  }

  async testAI(body: {
    provider?: string;
    model?: string;
    api_url?: string;
    api_key?: string;
    testPrompt?: string;
  }): Promise<ApiResponse<{ success: boolean; response?: string; error?: string; details?: string; provider?: string; model?: string }>> {
    return this.http.post<any>("/api/config/test-ai", body);
  }

  async testWebhook(body: {
    webhook_url?: string;
    "webhook.method"?: string;
    "webhook.content_type"?: string;
    "webhook.headers"?: string;
    "webhook.body_template"?: string;
    test_message?: string;
  }): Promise<ApiResponse<{ success: boolean; error?: string; details?: string }>> {
    return this.http.post<{ success: boolean; error?: string; details?: string }>("/api/config/test-webhook", body);
  }
}

/**
 * AI Config API methods (deprecated)
 */
class AIConfigAPI {
  constructor(private http: HttpClient) {}

  async get(): Promise<ApiResponse<AIConfig>> {
    return this.http.get<AIConfig>("/api/ai-config");
  }

  async update(body: Partial<AIConfig>): Promise<ApiResponse<void>> {
    return this.http.post<void>("/api/ai-config", body);
  }
}

export const PROXY_UPLOAD_LIMIT = 95 * 1024 * 1024;
export const MAX_UPLOAD_SIZE = 2 * 1024 * 1024 * 1024;
const DIRECT_UPLOAD_THRESHOLD = 8 * 1024 * 1024;

type PresignResponse =
  | { mode: "proxy" }
  | {
      mode: "direct";
      url: string;
      method: "PUT";
      headers: Record<string, string>;
      token: string;
    };

/**
 * Storage API methods
 */
class StorageAPI {
  constructor(private http: HttpClient) {}

  async uploadViaServer(file: File, key?: string): Promise<ApiResponse<UploadResponse>> {
    const formData = new FormData();
    formData.append("file", file);
    if (key) formData.append("key", key);

    return this.http.post<UploadResponse>("/api/storage", formData);
  }

  async upload(file: File, key?: string): Promise<ApiResponse<UploadResponse>> {
    if (file.size > MAX_UPLOAD_SIZE) {
      return { error: { status: 413, value: "文件不能超过 2GB" } };
    }

    let directError: string | undefined;
    if (file.size >= DIRECT_UPLOAD_THRESHOLD) {
      const presign = await this.http.post<PresignResponse>("/api/storage/presign", {
        name: key || file.name,
        size: file.size,
        type: file.type,
      });
      if (!presign.error && presign.data?.mode === "direct") {
        try {
          const response = await fetch(presign.data.url, {
            method: presign.data.method,
            headers: presign.data.headers,
            body: file,
          });
          if (!response.ok) {
            throw new Error(`存储返回 ${response.status}`);
          }
          return this.http.post<UploadResponse>("/api/storage/complete", { token: presign.data.token });
        } catch (error) {
          directError =
            error instanceof TypeError
              ? "直传失败，请检查存储桶是否已配置 CORS（允许当前站点 PUT 上传）"
              : error instanceof Error
                ? error.message
                : "直传失败";
          console.warn("[storage] direct upload failed, falling back to server upload:", error);
        }
      }
    }

    if (file.size > PROXY_UPLOAD_LIMIT) {
      return {
        error: {
          status: 413,
          value:
            directError ??
            `文件超过 ${Math.floor(PROXY_UPLOAD_LIMIT / 1024 / 1024)}MB，需要在「后台 → 存储管理」添加 S3 / R2 存储后才能上传大文件`,
        },
      };
    }

    return this.uploadViaServer(file, key);
  }
}

/**
 * Storage admin API methods
 */
class StorageAdminAPI {
  constructor(private http: HttpClient) {}

  list(): Promise<ApiResponse<StorageListResponse>> {
    return this.http.get<StorageListResponse>("/api/storages");
  }

  create(body: StorageInput): Promise<ApiResponse<StorageItem>> {
    return this.http.post<StorageItem>("/api/storages", body);
  }

  update(id: number, body: StorageInput): Promise<ApiResponse<StorageItem>> {
    return this.http.put<StorageItem>(`/api/storages/${id}`, body);
  }

  remove(id: number, force = false): Promise<ApiResponse<{ ok: boolean }>> {
    return this.http.delete<{ ok: boolean }>(`/api/storages/${id}${force ? "?force=1" : ""}`);
  }

  test(id: number): Promise<ApiResponse<StorageTestResponse>> {
    return this.http.post<StorageTestResponse>(`/api/storages/${id}/test`, {});
  }

  testConfig(body: StorageInput): Promise<ApiResponse<StorageTestResponse>> {
    return this.http.post<StorageTestResponse>("/api/storages/test-config", body);
  }

  testDefault(): Promise<ApiResponse<StorageTestResponse>> {
    return this.http.post<StorageTestResponse>("/api/storages/test-default", {});
  }
}

/**
 * Backup API methods (全量备份与容灾恢复)
 */
class BackupAPI {
  constructor(private http: HttpClient) {}

  // 1. 获取预备份统计信息
  getPreview(): Promise<ApiResponse<BackupPreviewResponse>> {
    return this.http.get<BackupPreviewResponse>("/api/backup/preview");
  }

  // 2. 本地下载
  async download(): Promise<{ ok: boolean; error?: string; filename?: string }> {
    const token = getAuthToken();
    const headers: Record<string, string> = {};
    if (token) headers["Authorization"] = `Bearer ${token}`;

    try {
      const res = await fetch(`${endpoint}/api/backup/export`, {
        method: "GET",
        headers,
        credentials: "include",
      });

      if (!res.ok) {
        const text = await res.text().catch(() => "");
        return { ok: false, error: text || `下载失败 (${res.status})` };
      }

      const blob = await res.blob();
      const disposition = res.headers.get("content-disposition") || "";
      const match = /filename="?([^"]+)"?/.exec(disposition);
      const filename = match ? match[1] : `rin-backup-${new Date().toISOString().slice(0, 10)}.json`;

      const url = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      window.URL.revokeObjectURL(url);
      return { ok: true, filename };
    } catch (e: any) {
      return { ok: false, error: e.message || "下载失败" };
    }
  }

  // 3. 归档到存储桶
  exportToStorage(): Promise<ApiResponse<BackupExportToStorageResponse>> {
    return this.http.post<BackupExportToStorageResponse>("/api/backup/export-to-storage", {});
  }

  // 4. 获取存储桶备份列表
  listStorageBackups(): Promise<ApiResponse<BackupStorageListResponse>> {
    return this.http.get<BackupStorageListResponse>("/api/backup/storage-list");
  }

  // 4.1 检查存储桶指定路径的备份文件
  inspectStorageBackup(key: string): Promise<ApiResponse<BackupInspectResponse>> {
    return this.http.post<BackupInspectResponse>("/api/backup/inspect-storage-backup", { key });
  }

  // 5. 从存储桶恢复
  restoreFromStorage(key: string): Promise<ApiResponse<BackupRestoreResponse>> {
    return this.http.post<BackupRestoreResponse>("/api/backup/restore-from-storage", { key });
  }

  // 6. 从本地文件恢复
  importFromFile(file: File): Promise<ApiResponse<BackupRestoreResponse>> {
    const formData = new FormData();
    formData.append("file", file);
    return this.http.post<BackupRestoreResponse>("/api/backup/import", formData);
  }

  // =================== GitHub 专属备份能力 ===================

  // 7. 获取 GitHub 备份配置
  getGitHubConfig(): Promise<ApiResponse<GitHubBackupConfigResponse>> {
    return this.http.get<GitHubBackupConfigResponse>("/api/backup/github-config");
  }

  // 8. 保存 GitHub 备份配置
  saveGitHubConfig(body: {
    owner: string;
    repo: string;
    branch?: string;
    path?: string;
    token?: string;
  }): Promise<ApiResponse<{ ok: boolean; message: string }>> {
    return this.http.post<{ ok: boolean; message: string }>("/api/backup/github-config", body);
  }

  // 9. 测试 GitHub 仓库连接与私有属性
  testGitHubConnection(): Promise<ApiResponse<{ ok: boolean; isPrivate: boolean; message: string; defaultBranch?: string }>> {
    return this.http.post<{ ok: boolean; isPrivate: boolean; message: string; defaultBranch?: string }>("/api/backup/github-test", {});
  }

  // 10. 备份到 GitHub 仓库
  exportToGitHub(): Promise<ApiResponse<{
    ok: boolean;
    filename: string;
    path: string;
    target: string;
    commitSha: string;
    htmlUrl: string;
    metadata: any;
  }>> {
    return this.http.post<any>("/api/backup/export-to-github", {});
  }

  // 11. 列出 GitHub 仓库备份列表
  listGitHubBackups(): Promise<ApiResponse<GitHubBackupListResponse>> {
    return this.http.get<GitHubBackupListResponse>("/api/backup/github-list");
  }

  // 12. 预览指定的 GitHub 备份
  inspectGitHubBackup(path: string): Promise<ApiResponse<BackupInspectResponse>> {
    return this.http.post<BackupInspectResponse>("/api/backup/inspect-github-backup", { path });
  }

  // 13. 从 GitHub 仓库恢复
  restoreFromGitHub(path: string): Promise<ApiResponse<BackupRestoreResponse>> {
    return this.http.post<BackupRestoreResponse>("/api/backup/restore-from-github", { path });
  }
}

/**
 * Search API methods
 */
class SearchAPI {
  constructor(private http: HttpClient) {}

  async search(keyword: string, params?: { page?: number; limit?: number }): Promise<ApiResponse<FeedListResponse>> {
    const searchParams = new URLSearchParams();
    if (params?.page) searchParams.set("page", params.page.toString());
    if (params?.limit) searchParams.set("limit", params.limit.toString());

    const query = searchParams.toString();
    return this.http.get<FeedListResponse>(`/api/search/${encodeURIComponent(keyword)}${query ? `?${query}` : ""}`);
  }
}

/**
 * Auth API methods
 */
class AuthAPI {
  constructor(private http: HttpClient) {}

  async status(): Promise<ApiResponse<AuthStatus>> {
    return this.http.get<AuthStatus>("/api/auth/status");
  }

  async login(body: LoginRequest): Promise<ApiResponse<LoginResponse>> {
    return this.http.post<LoginResponse>("/api/auth/login", body);
  }
}

/**
 * WordPress Import API methods
 */
class WordPressAPI {
  constructor(private http: HttpClient) {}

  async import(xml: string): Promise<ApiResponse<{ imported: number }>> {
    return this.http.post<{ imported: number }>("/api/wp", { xml });
  }
}

/**
 * RSS API methods
 */
class RSSAPI {
  constructor(private baseUrl: string) {}

  async getRSS(): Promise<string> {
    const response = await fetch(`${this.baseUrl}/rss.xml`);
    return response.text();
  }

  async getAtom(): Promise<string> {
    const response = await fetch(`${this.baseUrl}/atom.xml`);
    return response.text();
  }

  async getJSON(): Promise<unknown> {
    const response = await fetch(`${this.baseUrl}/rss.json`);
    return response.json();
  }
}

// ============================================================================
// Main API Client Class
// ============================================================================

export class ApiClient {
  private http: HttpClient;
  feed: FeedAPI;
  tag: TagAPI;
  comment: CommentAPI;
  user: UserAPI;
  friend: FriendAPI;
  moments: MomentsAPI;
  config: ConfigAPI;
  aiConfig: AIConfigAPI;
  storage: StorageAPI;
  storages: StorageAdminAPI;
  backup: BackupAPI;
  search: SearchAPI;
  auth: AuthAPI;
  wp: WordPressAPI;
  rss: RSSAPI;

  constructor(baseUrl: string) {
    this.http = new HttpClient(baseUrl);
    this.feed = new FeedAPI(this.http);
    this.tag = new TagAPI(this.http);
    this.comment = new CommentAPI(this.http);
    this.user = new UserAPI(this.http);
    this.friend = new FriendAPI(this.http);
    this.moments = new MomentsAPI(this.http);
    this.config = new ConfigAPI(this.http);
    this.aiConfig = new AIConfigAPI(this.http);
    this.storage = new StorageAPI(this.http);
    this.storages = new StorageAdminAPI(this.http);
    this.backup = new BackupAPI(this.http);
    this.search = new SearchAPI(this.http);
    this.auth = new AuthAPI(this.http);
    this.wp = new WordPressAPI(this.http);
    this.rss = new RSSAPI(baseUrl);
  }
}

export function createClient(baseUrl: string): ApiClient {
  return new ApiClient(baseUrl);
}

export default ApiClient;
```

---

### 文件三：`client/src/page/settings.tsx`

```tsx
import { Modal, SearchableSelect, SettingsBadge, SettingsCard, SettingsCardBody, SettingsCardHeader, SettingsCardRow } from "@rin/ui";
import { type ChangeEvent, useEffect, useMemo, useRef, useState } from "react";
import { Helmet } from "react-helmet";
import { useTranslation } from "react-i18next";
import ReactLoading from "react-loading";
import { client, oauth_url } from "../app/runtime";
import { Button } from "../components/button";
import { useAlert } from "../components/dialog";
import { HeaderLayoutPreview } from "../components/site-header/layout-preview";
import {
  HEADER_BEHAVIOR_OPTIONS,
  HEADER_LAYOUT_OPTIONS,
  normalizeHeaderBehavior,
  normalizeHeaderLayout,
} from "../components/site-header/layout-options";
import { FEED_CARD_VARIANTS, normalizeFeedCardVariant } from "../components/feed-card-options";
import { FeedCardPreview } from "../components/feed-card-preview";
import { FEED_LAYOUT_OPTIONS, normalizeFeedLayout } from "../components/feed-layout-options";
import { useSiteConfig } from "../hooks/useSiteConfig";
import { applyThemeColor, normalizeThemeColor } from "../utils/theme-color";
import { AISummarySettings } from "./settings-ai";
import { ItemButton, ItemImageInput, ItemInput, ItemSwitch, ItemTitle, ItemWithUpload } from "./settings-items";
import {
  areSettingsDraftsEqual,
  buildAIConfigDraftValue,
  createSettingsConfigWrappers,
  importWordPressFile,
  loadSettingsConfigState,
  mergeSessionConfig,
  saveSettingsConfigState,
  type SettingsDraft,
  updateDraftConfig,
  uploadFavicon,
} from "./settings-helpers";

import "../utils/thumb.css";

const WEBHOOK_METHOD_OPTIONS = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"].map((value) => ({
  label: value,
  value,
}));

const THEME_COLOR_OPTIONS = [
  { label: "Rose", value: "#fc466b" },
  { label: "Violet", value: "#7c3aed" },
  { label: "Blue", value: "#2563eb" },
  { label: "Teal", value: "#0f766e" },
  { label: "Orange", value: "#ea580c" },
];

export function Settings() {
  const { t } = useTranslation();
  const siteConfig = useSiteConfig();
  const [isOpen, setIsOpen] = useState(false);
  const [msg, setMsg] = useState("");
  const [msgList, setMsgList] = useState<{ title: string; reason: string }[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [testingWebhook, setTestingWebhook] = useState(false);
  const [webhookTestMessage, setWebhookTestMessage] = useState("");
  const [draft, setDraft] = useState<SettingsDraft>({ clientConfig: {}, serverConfig: {} });
  const [initialDraft, setInitialDraft] = useState<SettingsDraft>({ clientConfig: {}, serverConfig: {} });
  const [hasStoredAiApiKey, setHasStoredAiApiKey] = useState(false);
  const ref = useRef(false);
  const restoreFileRef = useRef<HTMLInputElement>(null);
  const initialDraftRef = useRef<SettingsDraft>({ clientConfig: {}, serverConfig: {} });
  const { showAlert, AlertUI } = useAlert();

  // ================= 备份 & 恢复交互状态 =================
  const [backupActionLoading, setBackupActionLoading] = useState(false);

  // 1. 备份前确认弹窗
  const [backupConfirmModal, setBackupConfirmModal] = useState<{
    open: boolean;
    type: "local" | "cloud" | "github";
    previewData?: any;
    targetStorage?: any;
    targetGitHub?: any;
  }>({ open: false, type: "local" });

  // 2. 备份成功通知弹窗
  const [backupSuccessModal, setBackupSuccessModal] = useState<{
    open: boolean;
    filename: string;
    path: string;
    target: string;
    counts: any;
    extraNote?: string;
  } | null>(null);

  // 3. 存储桶备份选择列表弹窗
  const [cloudBackupsModal, setCloudBackupsModal] = useState<{
    open: boolean;
    loading: boolean;
    items: any[];
    target: string;
    error?: string;
  }>({ open: false, loading: false, items: [], target: "" });

  const [manualCloudKey, setManualCloudKey] = useState("backups/rin-backup-2026-10-24T01-43-36.json");

  // 3.1 GitHub 备份选择列表弹窗
  const [githubBackupsModal, setGithubBackupsModal] = useState<{
    open: boolean;
    loading: boolean;
    items: any[];
    target: string;
    branch: string;
  }>({ open: false, loading: false, items: [], target: "", branch: "main" });

  // 3.2 GitHub 备份配置管理弹窗
  const [githubConfigModal, setGithubConfigModal] = useState<{
    open: boolean;
    owner: string;
    repo: string;
    branch: string;
    path: string;
    token: string;
    hasToken: boolean;
    configured: boolean;
    saving: boolean;
    testing: boolean;
    testResult?: string;
  }>({
    open: false,
    owner: "",
    repo: "",
    branch: "main",
    path: "backups",
    token: "",
    hasToken: false,
    configured: false,
    saving: false,
    testing: false,
  });

  // 4. 恢复前确认弹窗（包含待恢复数据预览）
  const [restoreConfirmModal, setRestoreConfirmModal] = useState<{
    open: boolean;
    source: "local" | "cloud" | "github";
    localFile?: File | null;
    cloudKey?: string;
    githubPath?: string;
    filename: string;
    exportedAt: string;
    counts: any;
  } | null>(null);

  function getDraftThemeColor(nextDraft: SettingsDraft) {
    return typeof nextDraft.clientConfig["theme.color"] === "string" ? nextDraft.clientConfig["theme.color"] : undefined;
  }

  useEffect(() => {
    if (ref.current) return;
    loadSettingsConfigState()
      .then((state) => {
        setDraft(state.draft);
        setInitialDraft(state.draft);
        initialDraftRef.current = state.draft;
        setHasStoredAiApiKey(state.hasStoredAiApiKey);
        mergeSessionConfig(state.draft.clientConfig);
        applyThemeColor(getDraftThemeColor(state.draft));
      })
      .catch((err: any) => {
        showAlert(t("settings.get_config_failed$message", { message: err.message }));
      })
      .finally(() => {
        setLoading(false);
      });
    ref.current = true;

    return () => {
      applyThemeColor(getDraftThemeColor(initialDraftRef.current));
    };
  }, [showAlert, t]);

  const { clientConfig, serverConfig } = useMemo(() => createSettingsConfigWrappers(draft), [draft]);
  const aiValue = useMemo(() => buildAIConfigDraftValue(draft, hasStoredAiApiKey), [draft, hasStoredAiApiKey]);
  const hasUnsavedChanges = !areSettingsDraftsEqual(draft, initialDraft);
  const themeColorValue = normalizeThemeColor(String(clientConfig.get("theme.color") ?? "#fc466b"));
  const feedLayoutValue = normalizeFeedLayout(String(clientConfig.get("feed.layout") ?? "list"));
  const feedCardVariantValue = normalizeFeedCardVariant(String(clientConfig.get("feed.card_variant") ?? "default"));
  const previewSiteName = String(clientConfig.get("site.name") ?? clientConfig.default("site.name") ?? "Rin");
  const previewSiteAvatar = String(clientConfig.get("site.avatar") ?? clientConfig.default("site.avatar") ?? "");

  function setConfigValue(type: "client" | "server", key: string, value: unknown) {
    setDraft((current) => updateDraftConfig(current, type, key, value));
  }

  function handleReset() {
    setDraft(initialDraft);
    applyThemeColor(getDraftThemeColor(initialDraft));
  }

  async function handleSave() {
    setSaving(true);
    try {
      const state = await saveSettingsConfigState(draft);
      setDraft(state.draft);
      setInitialDraft(state.draft);
      initialDraftRef.current = state.draft;
      setHasStoredAiApiKey(state.hasStoredAiApiKey || aiValue.apiKey.trim().length > 0);
      mergeSessionConfig(state.draft.clientConfig);
      window.dispatchEvent(new Event("storage"));
      showAlert(t("settings.ai_summary.save_success"));
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      showAlert(t("settings.update_failed$message", { message }));
    } finally {
      setSaving(false);
    }
  }

  async function handleFaviconChange(e: ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0];
    if (file) {
      await uploadFavicon(file, t, showAlert);
    }
  }

  async function onFileChange(e: ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0];
    if (file) {
      const { data, error } = await importWordPressFile(file);
      if (data) {
        setMsg(t("settings.import_success$success$skipped", { success: data.imported, skipped: 0 }));
        setMsgList([]);
        setIsOpen(true);
      } else if (error) {
        showAlert(t("settings.import_failed$message", { message: error.value }));
      }
    }
  }

  async function handleTestWebhook() {
    setTestingWebhook(true);
    try {
      const { data, error } = await client.config.testWebhook({
        webhook_url: String(serverConfig.get("webhook_url") ?? ""),
        "webhook.method": String(serverConfig.get("webhook.method") ?? ""),
        "webhook.content_type": String(serverConfig.get("webhook.content_type") ?? ""),
        "webhook.headers": String(serverConfig.get("webhook.headers") ?? ""),
        "webhook.body_template": String(serverConfig.get("webhook.body_template") ?? ""),
        test_message: webhookTestMessage,
      });

      if (error || !data || !data.success) {
        const message = error?.value || data?.error || t("settings.webhook.test.failed");
        const details = data?.details ? `\n${data.details}` : "";
        showAlert(`${message}${details}`);
        return;
      }

      showAlert(t("settings.webhook.test.success"));
    } finally {
      setTestingWebhook(false);
    }
  }

  // ================= 备份 & 恢复处理函数 =================

  // 1. 点击触发前置预览与确认弹窗
  const openBackupConfirm = async (type: "local" | "cloud" | "github") => {
    setBackupActionLoading(true);
    try {
      const { data } = await client.backup.getPreview();
      setBackupConfirmModal({
        open: true,
        type,
        previewData: data?.preview,
        targetStorage: data?.targetStorage,
        targetGitHub: data?.targetGitHub,
      });
    } catch {
      setBackupConfirmModal({ open: true, type });
    } finally {
      setBackupActionLoading(false);
    }
  };

  // 2. 确认开始执行备份
  const executeConfirmedBackup = async () => {
    const type = backupConfirmModal.type;
    setBackupConfirmModal({ ...backupConfirmModal, open: false });
    setBackupActionLoading(true);

    try {
      if (type === "local") {
        const res = await client.backup.download();
        if (!res.ok) {
          showAlert(res.error || "下载备份失败");
        } else {
          setBackupSuccessModal({
            open: true,
            filename: res.filename || "rin-backup-xxxx.json",
            path: "已下载至当前设备浏览器默认下载目录",
            target: "本地设备",
            counts: backupConfirmModal.previewData,
          });
        }
      } else if (type === "cloud") {
        const { data, error } = await client.backup.exportToStorage();
        if (error || !data || !data.ok) {
          showAlert(`存储桶备份失败: ${error?.value || "写入异常"}`);
        } else {
          setBackupSuccessModal({
            open: true,
            filename: data.filename,
            path: data.path,
            target: data.target,
            counts: data.metadata?.counts,
          });
          setManualCloudKey(data.path);
        }
      } else if (type === "github") {
        const { data, error } = await client.backup.exportToGitHub();
        if (error || !data || !data.ok) {
          showAlert(`GitHub 备份失败: ${error?.value || "提交异常，请检查 Token 权限"}`);
        } else {
          setBackupSuccessModal({
            open: true,
            filename: data.filename,
            path: data.path,
            target: data.target,
            counts: data.metadata?.counts,
            extraNote: `Commit SHA: ${data.commitSha}`,
          });
        }
      }
    } catch (e: any) {
      showAlert(`备份过程中出错: ${e.message}`);
    } finally {
      setBackupActionLoading(false);
    }
  };

  // 3. 打开存储桶备份历史选择列表
  const openCloudBackupsList = async () => {
    setCloudBackupsModal({ open: true, loading: true, items: [], target: "" });
    try {
      const { data, error } = await client.backup.listStorageBackups();
      if (error || !data || !data.ok) {
        showAlert(error?.value || "获取存储桶备份列表失败");
        setCloudBackupsModal({ open: false, loading: false, items: [], target: "" });
      } else {
        setCloudBackupsModal({
          open: true,
          loading: false,
          items: data.items || [],
          target: data.target || "云端存储桶",
          error: data.error,
        });
        if (data.items && data.items.length > 0) {
          setManualCloudKey(data.items[0].key);
        }
      }
    } catch (e: any) {
      showAlert(`读取异常: ${e.message}`);
      setCloudBackupsModal({ open: false, loading: false, items: [], target: "" });
    }
  };

  // 3.1 打开 GitHub 备份列表
  const openGitHubBackupsList = async () => {
    setGithubBackupsModal({ open: true, loading: true, items: [], target: "", branch: "main" });
    try {
      const { data, error } = await client.backup.listGitHubBackups();
      if (error || !data || !data.ok) {
        showAlert(error?.value || "获取 GitHub 备份列表失败，请先确认 GitHub 配置是否正确");
        setGithubBackupsModal({ open: false, loading: false, items: [], target: "", branch: "main" });
      } else {
        setGithubBackupsModal({
          open: true,
          loading: false,
          items: data.items || [],
          target: data.target,
          branch: data.branch,
        });
      }
    } catch (e: any) {
      showAlert(`读取 GitHub 异常: ${e.message}`);
      setGithubBackupsModal({ open: false, loading: false, items: [], target: "", branch: "main" });
    }
  };

  // 3.2 打开 GitHub 备份配置弹窗
  const openGitHubConfigDialog = async () => {
    setGithubConfigModal((prev) => ({ ...prev, open: true, testResult: undefined }));
    try {
      const { data } = await client.backup.getGitHubConfig();
      if (data) {
        setGithubConfigModal({
          open: true,
          owner: data.owner || "",
          repo: data.repo || "",
          branch: data.branch || "main",
          path: data.path || "backups",
          token: "",
          hasToken: data.hasToken,
          configured: data.configured,
          saving: false,
          testing: false,
        });
      }
    } catch {}
  };

  // 4. 选择本地 JSON 文件恢复
  const handleSelectLocalFile = async (e: ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    e.target.value = "";
    if (!file) return;

    try {
      const text = await file.text();
      const parsed = JSON.parse(text);
      if (!parsed || parsed.app !== "rin" || !parsed.data) {
        showAlert("无效的备份文件：该文件不是由 Rin 生成的标准数据镜像。");
        return;
      }
      setRestoreConfirmModal({
        open: true,
        source: "local",
        localFile: file,
        filename: file.name,
        exportedAt: parsed.exportedAt || "未知",
        counts: parsed.metadata?.counts || {
          feeds: parsed.data.feeds?.length ?? 0,
          comments: parsed.data.comments?.length ?? 0,
          moments: parsed.data.moments?.length ?? 0,
          friends: parsed.data.friends?.length ?? 0,
        },
      });
    } catch (err: any) {
      showAlert(`读取备份文件出错: ${err.message}`);
    }
  };

  // 5. 点击存储桶快照项核对
  const handleSelectCloudBackupItem = (item: any) => {
    setCloudBackupsModal({ ...cloudBackupsModal, open: false });
    setRestoreConfirmModal({
      open: true,
      source: "cloud",
      cloudKey: item.key,
      filename: item.filename,
      exportedAt: item.createdAt,
      counts: item.metadata?.counts || {},
    });
  };

  // 5.1 点击 GitHub 快照项核对
  const handleSelectGitHubBackupItem = async (item: any) => {
    setGithubBackupsModal({ ...githubBackupsModal, open: false });
    setBackupActionLoading(true);
    try {
      const { data, error } = await client.backup.inspectGitHubBackup(item.path);
      if (error || !data || !data.ok) {
        showAlert(`未能读取 GitHub 备份内容: ${error?.value || "文件解析错误"}`);
      } else {
        setRestoreConfirmModal({
          open: true,
          source: "github",
          githubPath: data.path,
          filename: data.filename,
          exportedAt: data.exportedAt,
          counts: data.metadata?.counts || {},
        });
      }
    } catch (e: any) {
      showAlert(`读取异常: ${e.message}`);
    } finally {
      setBackupActionLoading(false);
    }
  };

  // 6. 最终确认执行恢复操作
  const executeConfirmedRestore = async () => {
    if (!restoreConfirmModal) return;
    const { source, localFile, cloudKey, githubPath } = restoreConfirmModal;
    setRestoreConfirmModal(null);
    setBackupActionLoading(true);

    try {
      let resData: any = null;
      let resError: any = null;

      if (source === "local" && localFile) {
        const { data, error } = await client.backup.importFromFile(localFile);
        resData = data;
        resError = error;
      } else if (source === "cloud" && cloudKey) {
        const { data, error } = await client.backup.restoreFromStorage(cloudKey);
        resData = data;
        resError = error;
      } else if (source === "github" && githubPath) {
        const { data, error } = await client.backup.restoreFromGitHub(githubPath);
        resData = data;
        resError = error;
      }

      if (resError || !resData || !resData.ok) {
        showAlert(`恢复失败: ${resError?.value || resData?.error || "写入异常"}`);
      } else {
        const counts = resData.counts || {};
        showAlert(
          `🎉 全量数据恢复成功！\n成功还原: ${counts.feeds ?? 0} 篇文章、${counts.comments ?? 0} 条评论、${counts.moments ?? 0} 条动态、${counts.friends ?? 0} 个友链。\n点击确定将刷新页面。`,
          () => {
            window.location.reload();
          }
        );
      }
    } catch (err: any) {
      showAlert(`恢复过程出错: ${err.message}`);
    } finally {
      setBackupActionLoading(false);
    }
  };

  return (
    <div className="flex w-full flex-col">
      <Helmet>
        <title>{`${t("settings.title")} - ${siteConfig.name}`}</title>
      </Helmet>
      <main className="w-full rounded-2xl bg-w" aria-label={t("main_content")}>
        <div className="flex flex-col items-start space-y-2">
          {(loading || saving) && <ReactLoading width="1em" height="1em" type="spin" color="#FC466B" />}
          <ItemTitle title={t("settings.site.title")} />
          <ItemInput
            title={t("settings.site.name.title")}
            description={t("settings.site.name.desc")}
            configKeyTitle={t("settings.site.name.label")}
            value={String(clientConfig.get("site.name") ?? "")}
            placeholder={String(clientConfig.default("site.name") ?? t("settings.site.name.label"))}
            onChange={(value) => {
              setConfigValue("client", "site.name", value);
            }}
          />
          <ItemInput
            title={t("settings.site.description.title")}
            description={t("settings.site.description.desc")}
            configKeyTitle={t("settings.site.description.label")}
            value={String(clientConfig.get("site.description") ?? "")}
            placeholder={String(clientConfig.default("site.description") ?? t("settings.site.description.label"))}
            onChange={(value) => {
              setConfigValue("client", "site.description", value);
            }}
          />
          <ItemImageInput
            title={t("settings.site.avatar.title")}
            description={t("settings.site.avatar.desc")}
            configKeyTitle={t("settings.site.avatar.label")}
            value={String(clientConfig.get("site.avatar") ?? "")}
            placeholder={String(clientConfig.default("site.avatar") ?? t("settings.site.avatar.label"))}
            onChange={(value) => {
              setConfigValue("client", "site.avatar", value);
            }}
            onError={showAlert}
          />
          <ItemInput
            title={t("settings.site.page_size.title")}
            description={t("settings.site.page_size.desc")}
            configKeyTitle={t("settings.site.page_size.label")}
            value={String(clientConfig.get("site.page_size") ?? "")}
            placeholder={String(clientConfig.default("site.page_size") ?? t("settings.site.page_size.label"))}
            onChange={(value) => {
              setConfigValue("client", "site.page_size", value);
            }}
          />

          <ItemTitle title={t("settings.personalization.title")} />
          <div className="w-full">
            <SettingsCard>
              <SettingsCardRow
                header={
                  <SettingsCardHeader
                    title={t("settings.header_layout.title")}
                    description={t("settings.header_layout.desc")}
                  />
                }
                action={
                  <SearchableSelect
                    value={normalizeHeaderLayout(String(clientConfig.get("header.layout") ?? "classic"))}
                    onChange={(value) => {
                      setConfigValue("client", "header.layout", value);
                    }}
                    options={HEADER_LAYOUT_OPTIONS.map((value) => ({
                      value,
                      label: t(`settings.header_layout.options.${value}`),
                    }))}
                    placeholder={t("settings.header_layout.title")}
                    emptyLabel={t("no_more")}
                    searchable={false}
                  />
                }
              />
              <SettingsCardBody>
                <div className="grid gap-3 md:grid-cols-2">
                  {HEADER_LAYOUT_OPTIONS.map((value) => (
                    <HeaderLayoutPreview
                      key={value}
                      data={{
                        avatar: previewSiteAvatar,
                        name: previewSiteName,
                        themeColor: themeColorValue,
                      }}
                      layout={value}
                      selected={normalizeHeaderLayout(String(clientConfig.get("header.layout") ?? "classic")) === value}
                      title={t(`settings.header_layout.options.${value}`)}
                      description={t(`settings.header_layout.preview.${value}`)}
                      onClick={() => {
                        setConfigValue("client", "header.layout", value);
                      }}
                    />
                  ))}
                </div>
              </SettingsCardBody>
              <div className="mt-4 border-t border-black/5 pt-4 dark:border-white/10">
                <SettingsCardRow
                  header={
                    <SettingsCardHeader
                      title={t("settings.feed_layout.title")}
                      description={t("settings.feed_layout.desc")}
                    />
                  }
                  action={
                    <SearchableSelect
                      value={feedLayoutValue}
                      onChange={(value) => {
                        setConfigValue("client", "feed.layout", value);
                      }}
                      options={FEED_LAYOUT_OPTIONS.map((value) => ({
                        value,
                        label: t(`settings.feed_layout.options.${value}`),
                      }))}
                      placeholder={t("settings.feed_layout.title")}
                      emptyLabel={t("no_more")}
                      searchable={false}
                    />
                  }
                />
              </div>
              <div className="mt-4 border-t border-black/5 pt-4 dark:border-white/10">
                <SettingsCardRow
                  header={
                    <SettingsCardHeader
                      title={t("settings.feed_card.title")}
                      description={t("settings.feed_card.desc")}
                    />
                  }
                  action={
                    <SearchableSelect
                      value={feedCardVariantValue}
                      onChange={(value) => {
                        setConfigValue("client", "feed.card_variant", value);
                      }}
                      options={FEED_CARD_VARIANTS.map((value) => ({
                        value,
                        label: t(`settings.feed_card.options.${value}`),
                      }))}
                      placeholder={t("settings.feed_card.title")}
                      emptyLabel={t("no_more")}
                      searchable={false}
                    />
                  }
                />
                <SettingsCardBody>
                  <div className="grid gap-3 md:grid-cols-2">
                    {FEED_CARD_VARIANTS.map((value) => (
                      <FeedCardPreview
                        key={value}
                        variant={value}
                        selected={feedCardVariantValue === value}
                        title={t(`settings.feed_card.options.${value}`)}
                        description={t(`settings.feed_card.preview.${value}`)}
                        onClick={() => {
                          setConfigValue("client", "feed.card_variant", value);
                        }}
                      />
                    ))}
                  </div>
                </SettingsCardBody>
              </div>
              <div className="mt-4 border-t border-black/5 pt-4 dark:border-white/10">
                <SettingsCardRow
                  header={
                    <SettingsCardHeader
                      title={t("settings.theme_color.title")}
                      description={t("settings.theme_color.desc")}
                    />
                  }
                  action={
                    <div className="text-sm font-medium t-primary">{themeColorValue}</div>
                  }
                />
                <SettingsCardBody>
                  <div className="flex flex-wrap gap-3">
                    {THEME_COLOR_OPTIONS.map((option) => {
                      const selected = themeColorValue === option.value;
                      return (
                        <button
                          key={option.value}
                          type="button"
                          onClick={() => {
                            setConfigValue("client", "theme.color", option.value);
                            applyThemeColor(option.value);
                          }}
                          className={`flex items-center gap-3 rounded-xl border px-3 py-2 transition-all ${
                            selected
                              ? "border-theme bg-theme/5 shadow-sm shadow-theme/10"
                              : "border-black/10 hover:border-black/20 dark:border-white/10 dark:hover:border-white/20"
                          }`}
                        >
                          <span
                            className="h-6 w-6 rounded-full border border-black/10 dark:border-white/10"
                            style={{ backgroundColor: option.value }}
                          />
                          <span className="text-sm t-primary">{t(`settings.theme_color.options.${option.label.toLowerCase()}`)}</span>
                          {selected ? <i className="ri-check-line text-theme" /> : null}
                        </button>
                      );
                    })}
                    <label className="flex items-center gap-3 rounded-xl border border-black/10 px-3 py-2 hover:border-black/20 dark:border-white/10 dark:hover:border-white/20">
                      <input
                        type="color"
                        value={themeColorValue}
                        onChange={(event) => {
                          const normalized = normalizeThemeColor(event.target.value);
                          setConfigValue("client", "theme.color", normalized);
                          applyThemeColor(normalized);
                        }}
                        className="color-input-reset h-6 w-6 cursor-pointer rounded-full border-0 bg-transparent p-0"
                      />
                      <span className="text-sm t-primary">{t("settings.theme_color.custom")}</span>
                    </label>
                  </div>
                </SettingsCardBody>
              </div>
              <div className="mt-4 border-t border-black/5 pt-4 dark:border-white/10">
                <SettingsCardRow
                  header={
                    <SettingsCardHeader
                      title={t("settings.header_behavior.title")}
                      description={t("settings.header_behavior.desc")}
                    />
                  }
                  action={
                    <SearchableSelect
                      value={normalizeHeaderBehavior(String(clientConfig.get("header.behavior") ?? "fixed"))}
                      onChange={(value) => {
                        setConfigValue("client", "header.behavior", value);
                      }}
                      options={HEADER_BEHAVIOR_OPTIONS.map((value) => ({
                        value,
                        label: t(`settings.header_behavior.options.${value}`),
                      }))}
                      placeholder={t("settings.header_behavior.title")}
                      emptyLabel={t("no_more")}
                      searchable={false}
                    />
                  }
                />
              </div>
            </SettingsCard>
          </div>

          <ItemTitle title={t("settings.other.title")} />
          <ItemSwitch
            title={t("settings.login.enable.title")}
            description={t("settings.login.enable.desc", { url: oauth_url })}
            checked={clientConfig.getBoolean("login.enabled")}
            onChange={(checked) => {
              setConfigValue("client", "login.enabled", checked);
            }}
          />
          <ItemSwitch
            title={t("settings.comment.enable.title")}
            description={t("settings.comment.enable.desc")}
            checked={clientConfig.getBoolean("comment.enabled")}
            onChange={(checked) => {
              setConfigValue("client", "comment.enabled", checked);
            }}
          />
          <ItemSwitch
            title={t("settings.counter.enable.title")}
            description={t("settings.counter.enable.desc")}
            checked={clientConfig.getBoolean("counter.enabled")}
            onChange={(checked) => {
              setConfigValue("client", "counter.enabled", checked);
            }}
          />
          <ItemSwitch
            title={t("settings.rss.title")}
            description={t("settings.rss.desc")}
            checked={clientConfig.getBoolean("rss")}
            onChange={(checked) => {
              setConfigValue("client", "rss", checked);
            }}
          />
          <ItemWithUpload
            title={t("settings.favicon.title")}
            description={t("settings.favicon.desc")}
            accept="image/jpeg,image/png,image/gif,image/webp,image/svg+xml"
            onFileChange={handleFaviconChange}
          />
          <ItemInput
            title={t("settings.footer.title")}
            description={t("settings.footer.desc")}
            configKeyTitle="Footer HTML"
            value={String(clientConfig.get("footer") ?? "")}
            onChange={(value) => {
              setConfigValue("client", "footer", value);
            }}
          />

          <ItemTitle title={t("settings.webhook.title")} />
          <ItemInput
            title={t("settings.webhook.url.title")}
            description={t("settings.webhook.url.desc")}
            configKeyTitle="WEBHOOK_URL"
            value={String(serverConfig.get("webhook_url") ?? "")}
            placeholder="https://example.com/webhook"
            onChange={(value) => {
              setConfigValue("server", "webhook_url", value);
            }}
          />
          <div className="w-full">
            <SettingsCard>
              <SettingsCardRow
                header={
                  <SettingsCardHeader
                    title={t("settings.webhook.method.title")}
                    description={t("settings.webhook.method.desc")}
                  />
                }
                action={
                  <SearchableSelect
                    value={String(serverConfig.get("webhook.method") ?? "")}
                    onChange={(value) => {
                      setConfigValue("server", "webhook.method", value);
                    }}
                    options={WEBHOOK_METHOD_OPTIONS}
                    placeholder={String(serverConfig.default("webhook.method") ?? "POST")}
                    searchPlaceholder={t("settings.webhook.method.title")}
                    emptyLabel={t("no_more")}
                    allowCustomValue
                    customValueLabel={(value) => `${t("update.title")}: ${value}`}
                  />
                }
              />
            </SettingsCard>
          </div>
          <ItemInput
            title={t("settings.webhook.content_type.title")}
            description={t("settings.webhook.content_type.desc")}
            configKeyTitle="Content-Type"
            value={String(serverConfig.get("webhook.content_type") ?? "")}
            placeholder={String(serverConfig.default("webhook.content_type") ?? "application/json")}
            onChange={(value) => {
              setConfigValue("server", "webhook.content_type", value);
            }}
          />
          <ItemInput
            title={t("settings.webhook.headers.title")}
            description={t("settings.webhook.headers.desc")}
            configKeyTitle={t("settings.webhook.headers.label")}
            value={String(serverConfig.get("webhook.headers") ?? "")}
            placeholder={String(serverConfig.default("webhook.headers") ?? "{}")}
            onChange={(value) => {
              setConfigValue("server", "webhook.headers", value);
            }}
          />
          <ItemInput
            title={t("settings.webhook.body_template.title")}
            description={t("settings.webhook.body_template.desc")}
            configKeyTitle={t("settings.webhook.body_template.label")}
            value={String(serverConfig.get("webhook.body_template") ?? "")}
            placeholder={String(serverConfig.default("webhook.body_template") ?? "")}
            onChange={(value) => {
              setConfigValue("server", "webhook.body_template", value);
            }}
          />
          <div className="w-full">
            <SettingsCard>
              <SettingsCardRow
                header={
                  <SettingsCardHeader
                    title={t("settings.webhook.test.title")}
                    description={t("settings.webhook.test.desc")}
                  />
                }
                action={
                  <Button
                    title={testingWebhook ? t("settings.webhook.test.sending") : t("settings.webhook.test.button")}
                    onClick={handleTestWebhook}
                    disabled={testingWebhook}
                  />
                }
              />
              <SettingsCardBody>
                <textarea
                  value={webhookTestMessage}
                  placeholder={t("settings.webhook.test.placeholder")}
                  onChange={(event) => {
                    setWebhookTestMessage(event.target.value);
                  }}
                  className="min-h-28 w-full rounded-xl border border-black/10 bg-w px-4 py-3 text-sm t-primary outline-none transition-colors placeholder:text-neutral-400 focus:border-black/20 focus:ring-2 focus:ring-theme/10 dark:border-white/10 dark:placeholder:text-neutral-500 dark:focus:border-white/20"
                />
              </SettingsCardBody>
            </SettingsCard>
          </div>

          <ItemTitle title={t("settings.friend.title")} />
          <ItemSwitch
            title={t("settings.friend.apply.title")}
            description={t("settings.friend.apply.desc")}
            checked={Boolean(clientConfig.get("friend_apply_enable"))}
            onChange={(checked) => {
              setConfigValue("client", "friend_apply_enable", checked);
            }}
          />
          <ItemSwitch
            title={t("settings.friend.health.title")}
            description={t("settings.friend.health.desc")}
            checked={Boolean(serverConfig.get("friend_crontab"))}
            onChange={(checked) => {
              setConfigValue("server", "friend_crontab", checked);
            }}
          />
          <ItemInput
            title={t("settings.friend.health.ua.title")}
            description={t("settings.friend.health.ua.desc")}
            configKeyTitle="User-Agent"
            value={String(serverConfig.get("friend_ua") ?? "")}
            placeholder={String(serverConfig.default("friend_ua") ?? "User-Agent")}
            onChange={(value) => {
              setConfigValue("server", "friend_ua", value);
            }}
          />

          <ItemTitle title={t("settings.maintenance.title")} />

          {/* ============ ★ GitHub 异地私有仓库备份与恢复 ============ */}
          <div className="w-full">
            <SettingsCard tone="default">
              <SettingsCardRow
                header={
                  <SettingsCardHeader
                    title="GitHub 私有仓库自动备份（极度推荐 • 异地容灾）"
                    description="将博客所有数据直接作为一个版本提交到您的 GitHub 私有仓库中。完全独立于 Cloudflare 架构，具备永久提交历史，即便主机或域名丢失，数据依然安全无虞。"
                    badge={<SettingsBadge tone="success">跨云独立容灾</SettingsBadge>}
                  />
                }
                action={
                  <div className="flex flex-wrap gap-2">
                    <Button
                      secondary
                      title="配置 GitHub 仓库"
                      onClick={openGitHubConfigDialog}
                    />
                    <Button
                      title={backupActionLoading ? "请稍候..." : "备份到 GitHub 仓库"}
                      onClick={() => openBackupConfirm("github")}
                      disabled={backupActionLoading}
                    />
                    <Button
                      secondary
                      title="从 GitHub 恢复"
                      onClick={openGitHubBackupsList}
                      disabled={backupActionLoading}
                    />
                  </div>
                }
              />
            </SettingsCard>
          </div>

          {/* ============ ★ 一键全量数据备份模块 ============ */}
          <div className="w-full">
            <SettingsCard>
              <SettingsCardRow
                header={
                  <SettingsCardHeader
                    title="数据全量备份（本地 / 存储桶）"
                    description="将所有文章、草稿、分类、评论、动态、友链、站点设置打包。支持直接下载到手机/电脑，或存入已绑定的存储桶（保留最近 3 份）。"
                  />
                }
                action={
                  <div className="flex flex-wrap gap-2">
                    <Button
                      title={backupActionLoading ? "请稍候..." : "下载备份到本地"}
                      onClick={() => openBackupConfirm("local")}
                      disabled={backupActionLoading}
                    />
                    <Button
                      secondary
                      title={backupActionLoading ? "请稍候..." : "备份到存储桶"}
                      onClick={() => openBackupConfirm("cloud")}
                      disabled={backupActionLoading}
                    />
                  </div>
                }
              />
            </SettingsCard>
          </div>

          {/* ============ ★ 全量灾难恢复模块（本地 / 存储桶） ============ */}
          <div className="w-full">
            <SettingsCard tone="warning">
              <SettingsCardRow
                header={
                  <SettingsCardHeader
                    title="数据恢复与灾难迁移（本地 / 存储桶）"
                    description="从本地 .json 文件恢复，或从当前绑定的存储桶恢复历史快照。"
                    badge={<SettingsBadge tone="warning">覆盖式写入</SettingsBadge>}
                  />
                }
                action={
                  <div className="flex flex-wrap gap-2">
                    <input
                      ref={restoreFileRef}
                      type="file"
                      accept=".json,application/json"
                      className="hidden"
                      onChange={handleSelectLocalFile}
                    />
                    <Button
                      title="从本地文件恢复"
                      onClick={() => restoreFileRef.current?.click()}
                      disabled={backupActionLoading}
                    />
                    <Button
                      secondary
                      title="从存储桶恢复"
                      onClick={openCloudBackupsList}
                      disabled={backupActionLoading}
                    />
                  </div>
                }
              />
            </SettingsCard>
          </div>

          <ItemSwitch
            title={t("settings.cache.enabled.title")}
            description={t("settings.cache.enabled.desc")}
            checked={Boolean(clientConfig.getBoolean("cache.enabled"))}
            onChange={(checked) => {
              setConfigValue("client", "cache.enabled", checked);
            }}
          />
          <ItemButton
            title={t("settings.cache.clear.title")}
            description={t("settings.cache.clear.desc")}
            buttonTitle={t("clear")}
            onConfirm={async () => {
              await client.config.clearCache().then(({ error }) => {
                if (error) {
                  showAlert(t("settings.cache.clear_failed$message", { message: error.value }));
                }
              });
            }}
            alertTitle={t("settings.cache.clear.confirm.title")}
            alertDescription={t("settings.cache.clear.confirm.desc")}
          />
          <ItemWithUpload
            title={t("settings.wordpress.title")}
            description={t("settings.wordpress.desc")}
            accept="application/xml"
            onFileChange={onFileChange}
          />

          <AISummarySettings
            value={aiValue}
            onChange={(updates) => {
              if (updates.enabled !== undefined) {
                setConfigValue("server", "ai_summary.enabled", updates.enabled);
              }
              if (updates.provider !== undefined) {
                setConfigValue("server", "ai_summary.provider", updates.provider);
              }
              if (updates.model !== undefined) {
                setConfigValue("server", "ai_summary.model", updates.model);
              }
              if (updates.apiUrl !== undefined) {
                setConfigValue("server", "ai_summary.api_url", updates.apiUrl);
              }
              if (updates.apiKey !== undefined) {
                setConfigValue("server", "ai_summary.api_key", updates.apiKey);
              }
            }}
          />

          {hasUnsavedChanges && (
            <div className="sticky bottom-4 z-20 mt-6 w-full pb-2">
              <SettingsCard tone="warning">
              <SettingsCardRow
                header={
                  <SettingsCardHeader
                    title={t("settings.ai_summary.save.title")}
                    description={t("settings.ai_summary.unsaved_changes")}
                    badge={<SettingsBadge tone="warning">{t("settings.ai_summary.unsaved_changes")}</SettingsBadge>}
                  />
                }
                action={
                  <>
                    <Button secondary title={t("reset")} onClick={handleReset} disabled={saving} />
                    <Button title={t("save")} onClick={handleSave} disabled={saving || loading} />
                  </>
                }
              />
              </SettingsCard>
            </div>
          )}
        </div>
      </main>

      {/* 1. 备份前确认弹窗 */}
      <Modal
        isOpen={backupConfirmModal.open}
        onRequestClose={() => setBackupConfirmModal({ ...backupConfirmModal, open: false })}
        contentLabel="确认备份"
        panelClassName="p-6"
      >
        <div className="flex flex-col items-start w-full">
          <h1 className="text-xl font-bold tracking-[-0.02em] t-primary">
            {backupConfirmModal.type === "local"
              ? "确认导出本地备份？"
              : backupConfirmModal.type === "github"
                ? "确认备份到 GitHub 私有仓库？"
                : "确认备份到云端存储桶？"}
          </h1>
          <p className="mt-2 text-sm text-neutral-500">
            {backupConfirmModal.type === "local"
              ? "系统将把博客全部数据打包生成为一个 JSON 快照，并通过浏览器直接下载到您的电脑或手机中。"
              : backupConfirmModal.type === "github"
                ? "系统将把博客全部数据打包成快照，并通过 GitHub API 自动提交（Commit）到您绑定的 GitHub 私有仓库中。"
                : "系统将把博客全部数据打包，并上传归档到您当前已绑定的云端存储桶中（自动保留最新的 3 份备份，旧备份自动清理淘汰）。"}
          </p>

          <div className="mt-4 w-full rounded-xl bg-secondary p-4 space-y-2 text-sm t-primary">
            <p className="font-semibold text-xs uppercase tracking-wider text-theme">即将备份的数据概览：</p>
            <div className="grid grid-cols-2 gap-2 pt-1 text-xs sm:text-sm">
              <p>📄 文章数量: <span className="font-semibold">{backupConfirmModal.previewData?.feedsCount ?? "计算中..."}</span> 篇</p>
              <p>💬 评论条数: <span className="font-semibold">{backupConfirmModal.previewData?.commentsCount ?? "计算中..."}</span> 条</p>
              <p>⚡ 动态数量: <span className="font-semibold">{backupConfirmModal.previewData?.momentsCount ?? "计算中..."}</span> 条</p>
              <p>🤝 友链数量: <span className="font-semibold">{backupConfirmModal.previewData?.friendsCount ?? "计算中..."}</span> 个</p>
              <p>💾 存储配置: <span className="font-semibold">{backupConfirmModal.previewData?.storagesCount ?? "计算中..."}</span> 个</p>
            </div>
            <div className="pt-2 border-t border-black/5 dark:border-white/5 text-xs text-neutral-500">
              <p>📍 备份目标: <span className="font-semibold t-primary">
                {backupConfirmModal.type === "local"
                  ? "本地设备（浏览器下载文件）"
                  : backupConfirmModal.type === "github"
                    ? `GitHub 仓库: ${backupConfirmModal.targetGitHub?.repo || "未配置"}`
                    : `${backupConfirmModal.targetStorage?.name || "绑定的存储桶"} -> ${backupConfirmModal.targetStorage?.folder || "backups"}/`}
              </span></p>
            </div>
          </div>

          <div className="mt-6 flex w-full justify-end gap-2">
            <Button secondary title="取消" onClick={() => setBackupConfirmModal({ ...backupConfirmModal, open: false })} />
            <Button
              title={backupActionLoading ? "正在打包..." : "确认开始备份"}
              onClick={executeConfirmedBackup}
              disabled={backupActionLoading}
            />
          </div>
        </div>
      </Modal>

      {/* 2. 备份成功信息回执弹窗 */}
      <Modal
        isOpen={Boolean(backupSuccessModal?.open)}
        onRequestClose={() => setBackupSuccessModal(null)}
        contentLabel="备份成功"
        panelClassName="p-6"
      >
        {backupSuccessModal ? (
          <div className="flex flex-col items-start w-full">
            <div className="flex items-center gap-2 text-emerald-600 dark:text-emerald-400">
              <i className="ri-checkbox-circle-fill text-2xl" />
              <h1 className="text-xl font-bold tracking-[-0.02em] t-primary">备份打包成功！</h1>
            </div>
            <p className="mt-2 text-sm text-neutral-500">您的数据已经生成并安全写入，详细信息如下：</p>

            <div className="mt-4 w-full rounded-xl bg-secondary p-4 space-y-2 text-xs sm:text-sm t-primary">
              <p>🏷️ <b>备份文件名：</b> <span className="font-mono">{backupSuccessModal.filename}</span></p>
              <p>📦 <b>目标归档：</b> <span>{backupSuccessModal.target}</span></p>
              <p>📍 <b>存储路径：</b> <span className="font-mono break-all">{backupSuccessModal.path}</span></p>
              {backupSuccessModal.extraNote ? (
                <p>🔗 <b>附加信息：</b> <span className="font-mono">{backupSuccessModal.extraNote}</span></p>
              ) : null}
              {backupSuccessModal.counts ? (
                <div className="pt-2 border-t border-black/5 dark:border-white/5 grid grid-cols-2 gap-1 text-neutral-500">
                  <span>包含文章: {backupSuccessModal.counts.feeds ?? 0} 篇</span>
                  <span>包含评论: {backupSuccessModal.counts.comments ?? 0} 条</span>
                  <span>包含动态: {backupSuccessModal.counts.moments ?? 0} 条</span>
                  <span>包含友链: {backupSuccessModal.counts.friends ?? 0} 个</span>
                </div>
              ) : null}
            </div>

            <div className="mt-6 flex w-full justify-end">
              <Button title="好的，我知道了" onClick={() => setBackupSuccessModal(null)} />
            </div>
          </div>
        ) : null}
      </Modal>

      {/* 3.1 GitHub 备份配置管理弹窗 */}
      <Modal
        isOpen={githubConfigModal.open}
        onRequestClose={() => setGithubConfigModal({ ...githubConfigModal, open: false })}
        contentLabel="配置 GitHub 私有备份仓库"
        size="md"
        panelClassName="p-6"
      >
        <div className="flex flex-col items-start w-full gap-3">
          <div className="flex items-center gap-2">
            <i className="ri-github-fill text-2xl t-primary" />
            <h1 className="text-xl font-bold tracking-[-0.02em] t-primary">GitHub 私有仓库备份设置</h1>
          </div>
          <p className="text-xs text-neutral-500">
            建议在 GitHub 创建一个 <b>Private（私有）</b> 仓库用于备份。访问令牌（PAT）将使用 AES-GCM 强加密保存在数据库中。
          </p>

          <div className="w-full space-y-3 mt-2">
            <div>
              <label className="text-xs font-semibold t-primary block mb-1">GitHub 用户名或组织名 (Owner)</label>
              <input
                type="text"
                placeholder="例如：xmjerssss"
                value={githubConfigModal.owner}
                onChange={(e) => setGithubConfigModal({ ...githubConfigModal, owner: e.target.value })}
                className="w-full rounded-xl border border-black/10 bg-w px-3 py-2 text-sm t-primary outline-none focus:border-theme dark:border-white/10"
              />
            </div>

            <div>
              <label className="text-xs font-semibold t-primary block mb-1">仓库名称 (Repo)</label>
              <input
                type="text"
                placeholder="例如：blog-backups (必须为 Private 仓库)"
                value={githubConfigModal.repo}
                onChange={(e) => setGithubConfigModal({ ...githubConfigModal, repo: e.target.value })}
                className="w-full rounded-xl border border-black/10 bg-w px-3 py-2 text-sm t-primary outline-none focus:border-theme dark:border-white/10"
              />
            </div>

            <div className="grid grid-cols-2 gap-2">
              <div>
                <label className="text-xs font-semibold t-primary block mb-1">分支 (Branch)</label>
                <input
                  type="text"
                  placeholder="main"
                  value={githubConfigModal.branch}
                  onChange={(e) => setGithubConfigModal({ ...githubConfigModal, branch: e.target.value })}
                  className="w-full rounded-xl border border-black/10 bg-w px-3 py-2 text-sm t-primary outline-none focus:border-theme dark:border-white/10"
                />
              </div>
              <div>
                <label className="text-xs font-semibold t-primary block mb-1">目录路径 (Path)</label>
                <input
                  type="text"
                  placeholder="backups"
                  value={githubConfigModal.path}
                  onChange={(e) => setGithubConfigModal({ ...githubConfigModal, path: e.target.value })}
                  className="w-full rounded-xl border border-black/10 bg-w px-3 py-2 text-sm t-primary outline-none focus:border-theme dark:border-white/10"
                />
              </div>
            </div>

            <div>
              <label className="text-xs font-semibold t-primary block mb-1">
                GitHub 个人访问令牌 (Personal Access Token)
                {githubConfigModal.hasToken ? (
                  <span className="ml-2 text-emerald-500 font-normal">（已加密保存，留空为不修改）</span>
                ) : null}
              </label>
              <input
                type="password"
                placeholder={githubConfigModal.hasToken ? "••••••••••••••••" : "ghp_xxxxxxxxxxxxxxxxxxxx"}
                value={githubConfigModal.token}
                onChange={(e) => setGithubConfigModal({ ...githubConfigModal, token: e.target.value })}
                className="w-full rounded-xl border border-black/10 bg-w px-3 py-2 text-sm font-mono t-primary outline-none focus:border-theme dark:border-white/10"
              />
              <p className="text-[11px] text-neutral-400 mt-1">
                需要具有 <b>repo</b> 权限（或细粒度 Token 赋予目标私有仓库的 <b>Contents: Read and write</b> 权限）。
              </p>
            </div>

            {githubConfigModal.testResult ? (
              <p className="text-xs p-3 rounded-lg bg-secondary text-neutral-600 dark:text-neutral-300 font-mono">
                {githubConfigModal.testResult}
              </p>
            ) : null}
          </div>

          <div className="mt-4 flex w-full justify-between items-center">
            <Button
              secondary
              title={githubConfigModal.testing ? "测试中..." : "测试连接"}
              disabled={githubConfigModal.testing || githubConfigModal.saving}
              onClick={async () => {
                setGithubConfigModal((prev) => ({ ...prev, testing: true, testResult: "正在连接 GitHub 验证..." }));
                try {
                  const { data, error } = await client.backup.testGitHubConnection();
                  if (error || !data || !data.ok) {
                    setGithubConfigModal((prev) => ({
                      ...prev,
                      testing: false,
                      testResult: `❌ 失败: ${error?.value || "无法连接仓库，请检查 Token 权限"}`,
                    }));
                  } else {
                    setGithubConfigModal((prev) => ({
                      ...prev,
                      testing: false,
                      testResult: `✅ ${data.message}`,
                    }));
                  }
                } catch (e: any) {
                  setGithubConfigModal((prev) => ({ ...prev, testing: false, testResult: `❌ 异常: ${e.message}` }));
                }
              }}
            />

            <div className="flex gap-2">
              <Button secondary title="取消" onClick={() => setGithubConfigModal({ ...githubConfigModal, open: false })} />
              <Button
                title={githubConfigModal.saving ? "保存中..." : "保存配置"}
                disabled={githubConfigModal.saving || githubConfigModal.testing}
                onClick={async () => {
                  if (!githubConfigModal.owner.trim() || !githubConfigModal.repo.trim()) {
                    showAlert("请填写完整 GitHub 用户名和仓库名");
                    return;
                  }
                  if (!githubConfigModal.hasToken && !githubConfigModal.token.trim()) {
                    showAlert("请输入 GitHub Personal Access Token");
                    return;
                  }
                  setGithubConfigModal((prev) => ({ ...prev, saving: true }));
                  try {
                    const { data, error } = await client.backup.saveGitHubConfig({
                      owner: githubConfigModal.owner.trim(),
                      repo: githubConfigModal.repo.trim(),
                      branch: githubConfigModal.branch.trim(),
                      path: githubConfigModal.path.trim(),
                      token: githubConfigModal.token.trim() || undefined,
                    });
                    if (error || !data || !data.ok) {
                      showAlert(`保存失败: ${error?.value || "写入异常"}`);
                    } else {
                      showAlert("GitHub 备份配置保存成功！");
                      setGithubConfigModal((prev) => ({ ...prev, open: false, hasToken: true, configured: true }));
                    }
                  } catch (e: any) {
                    showAlert(`保存异常: ${e.message}`);
                  } finally {
                    setGithubConfigModal((prev) => ({ ...prev, saving: false }));
                  }
                }}
              />
            </div>
          </div>
        </div>
      </Modal>

      {/* 3.2 GitHub 备份选择列表弹窗 */}
      <Modal
        isOpen={githubBackupsModal.open}
        onRequestClose={() => setGithubBackupsModal({ ...githubBackupsModal, open: false })}
        contentLabel="从 GitHub 仓库恢复"
        size="lg"
        panelClassName="p-6"
      >
        <div className="flex flex-col items-start w-full">
          <div className="flex items-center gap-2">
            <i className="ri-github-fill text-2xl t-primary" />
            <h1 className="text-xl font-bold tracking-[-0.02em] t-primary">从 GitHub 仓库恢复数据</h1>
          </div>
          <p className="mt-1 text-sm text-neutral-500">
            仓库 <b>{githubBackupsModal.target}</b>（分支: {githubBackupsModal.branch}）中保存的快照：
          </p>

          <div className="mt-4 w-full max-h-80 overflow-y-auto space-y-3">
            {githubBackupsModal.loading ? (
              <div className="flex items-center justify-center p-8 text-neutral-400 gap-2">
                <ReactLoading type="spin" color="#FC466B" width={20} height={20} />
                <span>正在获取 GitHub 仓库快照...</span>
              </div>
            ) : githubBackupsModal.items.length === 0 ? (
              <div className="p-4 rounded-xl bg-secondary text-sm text-neutral-500">
                <p>该仓库指定目录中暂未发现任何备份快照文件（以 rin-backup- 开头的 json 文件）。</p>
              </div>
            ) : (
              githubBackupsModal.items.map((item, idx) => (
                <div
                  key={item.sha || idx}
                  className="flex flex-col sm:flex-row sm:items-center justify-between p-4 rounded-xl border border-black/10 dark:border-white/10 bg-secondary gap-3"
                >
                  <div className="min-w-0">
                    <p className="font-mono text-sm font-semibold truncate t-primary">{item.filename}</p>
                    <p className="text-xs text-neutral-400 mt-1">
                      备份时间: {item.createdAt} · 大小: {Math.round((item.size || 0) / 1024)} KB · Commit SHA: {item.sha?.slice(0, 7)}
                    </p>
                  </div>
                  <Button
                    title="选择此备份"
                    onClick={() => handleSelectGitHubBackupItem(item)}
                  />
                </div>
              ))
            )}
          </div>

          <div className="mt-6 flex w-full justify-end">
            <Button secondary title="关闭" onClick={() => setGithubBackupsModal({ ...githubBackupsModal, open: false })} />
          </div>
        </div>
      </Modal>

      {/* 3.3 存储桶备份历史选择列表弹窗 */}
      <Modal
        isOpen={cloudBackupsModal.open}
        onRequestClose={() => setCloudBackupsModal({ ...cloudBackupsModal, open: false })}
        contentLabel="选择存储桶备份恢复"
        size="lg"
        panelClassName="p-6"
      >
        <div className="flex flex-col items-start w-full">
          <h1 className="text-xl font-bold tracking-[-0.02em] t-primary">从存储桶恢复数据</h1>
          <p className="mt-1 text-sm text-neutral-500">
            当前存储桶（{cloudBackupsModal.target}）历史备份快照：
          </p>

          <div className="mt-4 w-full max-h-80 overflow-y-auto space-y-3">
            {cloudBackupsModal.loading ? (
              <div className="flex items-center justify-center p-8 text-neutral-400 gap-2">
                <ReactLoading type="spin" color="#FC466B" width={20} height={20} />
                <span>正在读取存储桶...</span>
              </div>
            ) : cloudBackupsModal.items.length === 0 ? (
              <div className="p-4 rounded-xl bg-secondary text-sm text-neutral-500 space-y-2">
                <p>存储桶索引暂未获取到，但文件已在存储桶中。请在下方直接输入完整路径恢复。</p>
              </div>
            ) : (
              cloudBackupsModal.items.map((item, idx) => (
                <div
                  key={item.key || idx}
                  className="flex flex-col sm:flex-row sm:items-center justify-between p-4 rounded-xl border border-black/10 dark:border-white/10 bg-secondary gap-3"
                >
                  <div className="min-w-0">
                    <p className="font-mono text-sm font-semibold truncate t-primary">{item.filename}</p>
                    <p className="text-xs text-neutral-400 mt-1">
                      备份时间: {new Date(item.createdAt).toLocaleString()} · 包含文章: {item.metadata?.counts?.feeds ?? 0} 篇
                    </p>
                  </div>
                  <Button
                    title="选择此备份"
                    onClick={() => handleSelectCloudBackupItem(item)}
                  />
                </div>
              ))
            )}
          </div>

          {/* 手动指定云端文件路径恢复兜底 */}
          <div className="mt-5 w-full pt-4 border-t border-black/5 dark:border-white/5 space-y-2">
            <p className="text-xs font-semibold text-neutral-400 uppercase tracking-wider">
              或直接指定存储桶备份文件完整路径恢复：
            </p>
            <div className="flex gap-2">
              <input
                type="text"
                placeholder="例如：backups/rin-backup-2026-10-24T01-43-36.json"
                value={manualCloudKey}
                onChange={(e) => setManualCloudKey(e.target.value)}
                className="w-full rounded-xl border border-black/10 bg-w px-3 py-2 text-xs font-mono t-primary outline-none focus:border-theme dark:border-white/10"
              />
              <Button
                title={backupActionLoading ? "读取中..." : "读取并恢复"}
                disabled={backupActionLoading}
                onClick={async () => {
                  const key = manualCloudKey.trim();
                  if (!key) {
                    showAlert("请输入完整文件路径，例如：backups/rin-backup-2026-10-24T01-43-36.json");
                    return;
                  }
                  setBackupActionLoading(true);
                  try {
                    const { data, error } = await client.backup.inspectStorageBackup(key);
                    if (error || !data || !data.ok) {
                      showAlert(`未能读取到备份文件: ${error?.value || "路径错误或文件不存在"}`);
                    } else {
                      setCloudBackupsModal({ ...cloudBackupsModal, open: false });
                      setRestoreConfirmModal({
                        open: true,
                        source: "cloud",
                        cloudKey: data.key,
                        filename: data.filename,
                        exportedAt: data.exportedAt,
                        counts: data.metadata?.counts || {},
                      });
                    }
                  } catch (e: any) {
                    showAlert(`读取失败: ${e.message}`);
                  } finally {
                    setBackupActionLoading(false);
                  }
                }}
              />
            </div>
            <p className="text-[11px] text-neutral-400">
              *注意：路径中<b>不需要包含存储桶名字</b>，必须带有 <b>.json</b> 后缀。
            </p>
          </div>

          <div className="mt-6 flex w-full justify-end">
            <Button secondary title="关闭" onClick={() => setCloudBackupsModal({ ...cloudBackupsModal, open: false })} />
          </div>
        </div>
      </Modal>

      {/* 4. 恢复前确认弹窗（展示备份内详细内容并提示覆盖警告） */}
      <Modal
        isOpen={Boolean(restoreConfirmModal?.open)}
        onRequestClose={() => setRestoreConfirmModal(null)}
        contentLabel="确认恢复备份"
        panelClassName="p-6"
      >
        {restoreConfirmModal ? (
          <div className="flex flex-col items-start w-full">
            <div className="flex items-center gap-2 text-rose-600 dark:text-rose-400">
              <i className="ri-error-warning-fill text-2xl" />
              <h1 className="text-xl font-bold tracking-[-0.02em] t-primary">确认恢复此份备份？</h1>
            </div>
            <p className="mt-2 text-sm text-rose-600 dark:text-rose-300 font-medium">
              ⚠️ 高风险覆盖警示：恢复将用此备份包彻底覆盖当前站点的文章、动态、评论、友链和自定义配置！此操作无法撤销！
            </p>

            <div className="mt-4 w-full rounded-xl bg-secondary p-4 space-y-2 text-xs sm:text-sm t-primary">
              <p className="font-semibold text-xs uppercase tracking-wider text-theme">即将恢复的数据镜像详情：</p>
              <p>📦 <b>恢复来源：</b> {
                restoreConfirmModal.source === "local"
                  ? "本地上传文件"
                  : restoreConfirmModal.source === "github"
                    ? "GitHub 仓库快照"
                    : "云端存储桶文件"
              }</p>
              <p>🏷️ <b>备份文件名：</b> <span className="font-mono">{restoreConfirmModal.filename}</span></p>
              <p>🕒 <b>备份生成时间：</b> <span>{new Date(restoreConfirmModal.exportedAt).toLocaleString()}</span></p>
              <div className="pt-2 border-t border-black/5 dark:border-white/5 grid grid-cols-2 gap-2 text-xs">
                <span>📄 文章: {restoreConfirmModal.counts?.feeds ?? 0} 篇</span>
                <span>💬 评论: {restoreConfirmModal.counts?.comments ?? 0} 条</span>
                <span>⚡ 动态: {restoreConfirmModal.counts?.moments ?? 0} 条</span>
                <span>🤝 友链: {restoreConfirmModal.counts?.friends ?? 0} 个</span>
              </div>
            </div>

            <div className="mt-6 flex w-full justify-end gap-2">
              <Button secondary title="取消" onClick={() => setRestoreConfirmModal(null)} disabled={backupActionLoading} />
              <Button
                title={backupActionLoading ? "正在恢复写入..." : "确认覆盖并恢复"}
                onClick={executeConfirmedRestore}
                disabled={backupActionLoading}
              />
            </div>
          </div>
        ) : null}
      </Modal>

      {/* WordPress 导入弹窗 */}
      <Modal
        isOpen={isOpen}
        onRequestClose={() => setIsOpen(false)}
        contentLabel={t("settings.import_result")}
        panelClassName="p-6"
      >
        <div className="flex flex-col items-start">
          <h1 className="text-xl font-bold tracking-[-0.02em] t-primary">{t("settings.import_result")}</h1>
          <p className="mt-2 text-base t-primary">{msg}</p>
          <div className="flex w-full flex-col items-start">
            <p className="mt-4 text-base font-bold t-primary">{t("settings.import_skipped")}</p>
            <ul className="flex max-h-64 w-full flex-col items-start overflow-auto">
              {msgList.map((item, idx) => (
                <li key={idx} className="text-sm t-primary">
                  {t("settings.import_skipped_item$title$reason", { title: item.title, reason: item.reason })}
                </li>
              ))}
            </ul>
          </div>
          <div className="mt-4 flex w-full flex-col items-center">
            <button
              onClick={() => {
                setIsOpen(false);
              }}
              className="h-min rounded-xl bg-theme px-8 py-2 text-white"
            >
              {t("close")}
            </button>
          </div>
        </div>
      </Modal>
      <AlertUI />
    </div>
  );
}
```
Citations

https://github.com/openRin/Rin
https://github.com/kingcun/blog
info
Google AI models may make mistakes, so double-check outputs.
Use Arrow Up and Arrow Down to select a turn, Enter to jump to it, and Escape to return to the chat.
Start typing a prompt

