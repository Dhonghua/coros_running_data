import asyncio
import hashlib
import os
import time
import aiofiles
import httpx


# ========== 用户配置 ==========
COROS_ACCOUNT = "1025164368@qq.com"  # 👈 在此填写你的 Coros 登录邮箱
COROS_PASSWORD = "3693292qw"          # 👈 在此填写你的 Coros 登录密码（明文）

# 只下载跑步类运动（100~103），若要下载所有类型，改为 False
ONLY_RUN = True

# 下载并发数量
MAX_CONCURRENCY = 10
# =============================

FIT_FOLDER = r"E:/test/run_fit/fit_files"
COROS_URL_DICT = {
    "LOGIN_URL": "https://teamcnapi.coros.com/account/login",
    "DOWNLOAD_URL": "https://teamcnapi.coros.com/activity/detail/download",
    "ACTIVITY_LIST": "https://teamcnapi.coros.com/activity/query",
}

TIME_OUT = httpx.Timeout(240.0, connect=360.0)


class Coros:
    def __init__(self, account, password, is_only_running=False):
        self.account = account
        self.password = password
        self.headers = None
        self.req = None
        self.is_only_running = is_only_running

    async def login(self):
        """登录 Coros 并获取 access token"""
        url = COROS_URL_DICT["LOGIN_URL"]
        headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json;charset=UTF-8",
            "origin": "https://t.coros.com",
            "referer": "https://t.coros.com/",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        }
        data = {"account": self.account, "accountType": 2, "pwd": self.password}
        async with httpx.AsyncClient(timeout=TIME_OUT) as client:
            resp = await client.post(url, json=data, headers=headers)
            resp_json = resp.json()
            access_token = resp_json.get("data", {}).get("accessToken")
            if not access_token:
                raise Exception("❌ 登录失败，请检查账号或密码。")

            self.headers = {
                "accesstoken": access_token,
                "cookie": f"CPL-coros-region=2; CPL-coros-token={access_token}",
            }
            self.req = httpx.AsyncClient(timeout=TIME_OUT, headers=self.headers)
        print("✅ 登录成功。")

    async def fetch_activity_ids(self):
        """获取活动 ID 列表"""
        page_number = 1
        all_ids = []
        mode_list_str = "100,101,102,103" if self.is_only_running else ""

        while True:
            url = f"{COROS_URL_DICT['ACTIVITY_LIST']}?&modeList={mode_list_str}&pageNumber={page_number}&size=20&startDay=20251030&endDay=20251030"
            response = await self.req.get(url)
            data = response.json()
            activities = data.get("data", {}).get("dataList", None)
            if not activities:
                break

            for act in activities:
                label_id = act.get("labelId")
                if label_id:
                    all_ids.append(label_id)
            print(f"📄 已获取 {len(all_ids)} 个活动 ID")
            page_number += 1

        return all_ids

    async def download_activity(self, label_id):
        """下载单个活动 .fit 文件"""
        download_url = f"{COROS_URL_DICT['DOWNLOAD_URL']}?labelId={label_id}&sportType=100&fileType=4"
        try:
            response = await self.req.post(download_url)
            resp_json = response.json()
            file_url = resp_json.get("data", {}).get("fileUrl")
            if not file_url:
                print(f"⚠️ 无法获取下载链接：{label_id}")
                return None, None

            file_name = os.path.basename(file_url)
            file_path = os.path.join(FIT_FOLDER, file_name)

            async with self.req.stream("GET", file_url) as res:
                res.raise_for_status()
                async with aiofiles.open(file_path, "wb") as f:
                    async for chunk in res.aiter_bytes():
                        await f.write(chunk)
            print(f"✅ 下载完成: {file_name}")
            return label_id, file_name

        except Exception as e:
            print(f"❌ 下载失败 {label_id}: {e}")
            return None, None


def get_downloaded_ids(folder):
    """列出已下载文件名（不含扩展名）"""
    if not os.path.exists(folder):
        os.makedirs(folder)
    return [i.split(".")[0] for i in os.listdir(folder) if not i.startswith(".")]


async def gather_with_concurrency(n, tasks):
    """限制并发执行"""
    semaphore = asyncio.Semaphore(n)

    async def sem_task(task):
        async with semaphore:
            return await task

    return await asyncio.gather(*(sem_task(t) for t in tasks))


async def main():
    folder = FIT_FOLDER
    downloaded_ids = get_downloaded_ids(folder)

    encrypted_pwd = hashlib.md5(COROS_PASSWORD.encode()).hexdigest()
    coros = Coros(COROS_ACCOUNT, encrypted_pwd, is_only_running=ONLY_RUN)
    await coros.login()

    activity_ids = await coros.fetch_activity_ids()
    to_download = list(set(activity_ids) - set(downloaded_ids))
    print(f"\n共找到 {len(activity_ids)} 个活动，其中 {len(to_download)} 个未下载。\n")

    start_time = time.time()
    await gather_with_concurrency(MAX_CONCURRENCY, [coros.download_activity(i) for i in to_download])
    print(f"\n🏁 所有下载完成，用时 {time.time() - start_time:.1f} 秒。\n")

    await coros.req.aclose()


if __name__ == "__main__":
    asyncio.run(main())
