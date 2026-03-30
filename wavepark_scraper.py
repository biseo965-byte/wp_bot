"""
웨이브파크 시즌 기반 스케줄 + 잔여좌석 스크래퍼 (비동기 병렬)
=============================================================
날짜 단위 병렬 처리 — asyncio + aiohttp

병렬 구조:
  날짜 전체 동시 처리 (Semaphore로 동시 요청 수 제한)
    └── 각 날짜 내 상품별 getDetail 동시 처리
          └── 각 슬롯별 getWaveZone 동시 처리

설치:
    pip install aiohttp beautifulsoup4

실행:
    python wavepark_scraper.py
    python wavepark_scraper.py --concurrency 10   # 동시 요청 수 조정 (기본 8)
    python wavepark_scraper.py --sectype 30       # 자유서핑만
    python wavepark_scraper.py --out result.json  # JSON 저장
    python wavepark_scraper.py --available-only   # 잔여석 있는 것만
    python wavepark_scraper.py --season 2         # 특정 시즌 강제
    python wavepark_scraper.py --list-seasons     # 시즌 목록 확인
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import time
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

# .env 파일 자동 로드 (python-dotenv 설치 시)
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

import aiohttp
from bs4 import BeautifulSoup

from wavepark_dto import (
    BadgeType,
    ItemDetail,
    SecType,
    SurfZone,
    TimeSlot,
    WavePosition,
    WaveparkItem,
    WaveZoneResult,
)

# ──────────────────────────────────────────────
# 설정
# ──────────────────────────────────────────────

BASE_URL    = "https://www.wavepark.co.kr"
HEADERS     = {
    "User-Agent": "Mozilla/5.0 (compatible; wavepark-scraper/1.0)",
    "Referer":    f"{BASE_URL}/bookingone",
}

# 동시 요청 수 제한 (너무 높이면 서버가 503 반환할 수 있음)
DEFAULT_CONCURRENCY = 8

# 현재 시즌 종료 며칠 전부터 다음 시즌도 수집할지
NEXT_SEASON_LOOKAHEAD_DAYS = 14

# 시즌 정보 파일
SEASONS_FILE = Path(__file__).parent / "wavepark_seasons.json"

# 수집 대상 상품 (itemIdx 기준)
# 추가·제거 시 이 딕셔너리만 수정하세요.
TARGET_ITEMS: dict[str, str] = {
    "13":    "리프자유서핑(초급)",   # secType 30
    "14":    "리프자유서핑(중급)",   # secType 30
    "18":    "리프자유서핑(상급)",   # secType 30
    "27809": "Lv.4 라인업레슨",      # secType 70
    "27808": "Lv.5 턴기초레슨",      # secType 70
}

logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt = "%H:%M:%S",
)
log = logging.getLogger("wavepark")


# ──────────────────────────────────────────────
# 시즌 관리
# ──────────────────────────────────────────────

@dataclass
class Season:
    number:        int
    start:         date
    end:           date
    label:         str
    wave_schedule: dict = field(default_factory=dict)  # {"weekday": {"09:00": ["M1","M2"], ...}, "weekend": {...}}

    def get_wave_tags(self, pick_date: str, pick_datetime: str) -> list[str]:
        """날짜(평일/주말)와 시간으로 wave_tags 반환. 매핑 없으면 빈 리스트."""
        from datetime import date as _date
        day_type   = "weekend" if _date.fromisoformat(pick_date).weekday() >= 5 else "weekday"
        time_str   = pick_datetime[11:16]   # "2026-04-26 12:00:00" → "12:00"
        return self.wave_schedule.get(day_type, {}).get(time_str, [])

    @property
    def is_active(self) -> bool:
        return self.start <= date.today() <= self.end

    @property
    def days_until_end(self) -> int:
        return (self.end - date.today()).days

    def date_range(self) -> list[date]:
        """시즌 전체 날짜 (오픈 전: start~end, 진행 중: today~end)"""
        today = date.today()
        s = self.start if today < self.start else today
        if s > self.end:
            return []
        return [s + timedelta(days=i) for i in range((self.end - s).days + 1)]


def load_seasons() -> list[Season]:
    if not SEASONS_FILE.exists():
        _create_default_seasons_file()
    with open(SEASONS_FILE, encoding="utf-8") as f:
        raw = json.load(f)
    return sorted([
        Season(
            number        = s["number"],
            start         = date.fromisoformat(s["start"]),
            end           = date.fromisoformat(s["end"]),
            label         = s["label"],
            wave_schedule = s.get("wave_schedule", {}),
        )
        for s in raw["seasons"]
    ], key=lambda s: s.number)


def _create_default_seasons_file() -> None:
    default = {
        "_comment": "시즌 추가 시 seasons 배열에 항목을 추가하세요.",
        "seasons": [
            {"number": 1, "start": "2026-04-25", "end": "2026-06-05", "label": "1시즌 (봄)"}
        ]
    }
    with open(SEASONS_FILE, "w", encoding="utf-8") as f:
        json.dump(default, f, ensure_ascii=False, indent=2)
    log.info(f"시즌 파일 생성: {SEASONS_FILE}")


def resolve_collect_dates(
    seasons:      list[Season],
    force_season: Optional[int] = None,
    window:       int           = 15,
) -> list[tuple[date, Season]]:
    """
    수집할 (날짜, 시즌) 쌍 목록을 결정합니다.

    규칙:
      - force_season 지정 → 해당 시즌 전체 날짜
      - 오픈 전            → 가장 빠른 예정 시즌의 start ~ end 전체
      - 오픈 후            → 오늘부터 window(15)일
                            15일 내 현재 시즌이 종료되면
                            남은 일수만큼 다음 시즌에서 보충

    Returns:
        [(날짜, 해당 시즌), ...] 오름차순
    """
    today  = date.today()
    result: list[tuple[date, Season]] = []

    # ── force_season: 해당 시즌 전체 ──────────────
    if force_season is not None:
        matched = next((s for s in seasons if s.number == force_season), None)
        if not matched:
            raise ValueError(f"시즌 {force_season}이 wavepark_seasons.json에 없습니다.")
        for d in matched.date_range():
            result.append((d, matched))
        log.info(
            f"[강제] {matched.label}  "
            f"{matched.date_range()[0]} ~ {matched.date_range()[-1]}"
            f"  ({len(result)}일)"
        )
        return result

    current = next((s for s in seasons if s.start <= today <= s.end), None)

    # ── 오픈 전 ───────────────────────────────────
    if current is None:
        upcoming = next((s for s in seasons if s.start > today), None)
        if not upcoming:
            log.warning("등록된 시즌이 없거나 모든 시즌이 종료되었습니다.")
            return result

        days_to_open = (upcoming.start - today).days
        # 오픈일부터 window일 (시즌 종료일 초과 시 시즌 end로 제한)
        for i in range(window):
            d = upcoming.start + timedelta(days=i)
            if d > upcoming.end:
                break
            result.append((d, upcoming))

        log.info(
            f"오픈 전 — {upcoming.label}  "
            f"(D-{days_to_open})  "
            f"오픈일({upcoming.start})부터 {len(result)}일 수집"
        )
        return result

    # ── 오픈 후: 오늘부터 window일 ────────────────
    window_end = today + timedelta(days=window - 1)

    if current.end >= window_end:
        # 현재 시즌이 window 안에 충분히 남아 있음
        for i in range(window):
            result.append((today + timedelta(days=i), current))
        log.info(
            f"현재 시즌: {current.label}  "
            f"종료까지 {current.days_until_end}일  →  "
            f"{today} ~ {window_end} ({window}일) 수집"
        )
    else:
        # 현재 시즌이 window 도중 종료 → 다음 시즌으로 보충
        days_in_current = (current.end - today).days + 1
        days_in_next    = window - days_in_current

        for i in range(days_in_current):
            result.append((today + timedelta(days=i), current))

        next_s = next((s for s in seasons if s.number == current.number + 1), None)
        if next_s:
            for i in range(days_in_next):
                d = next_s.start + timedelta(days=i)
                if d <= next_s.end:
                    result.append((d, next_s))
            log.info(
                f"현재 시즌: {current.label}  "
                f"{today} ~ {current.end} ({days_in_current}일)  +  "
                f"다음 시즌: {next_s.label}  "
                f"{next_s.start} ~ {next_s.start + timedelta(days=days_in_next-1)}"
                f" ({days_in_next}일)  →  총 {len(result)}일 수집"
            )
        else:
            log.info(
                f"현재 시즌: {current.label}  "
                f"{today} ~ {current.end} ({days_in_current}일) 수집  "
                f"(다음 시즌 미등록)"
            )
            if current.days_until_end <= NEXT_SEASON_LOOKAHEAD_DAYS:
                log.warning("시즌 종료 임박 — wavepark_seasons.json에 다음 시즌을 추가해주세요.")

    return result


# ──────────────────────────────────────────────
# 결과 DTO
# ──────────────────────────────────────────────

@dataclass
class ZoneSlot:
    season_number:  int
    season_label:   str
    item_idx:       str
    sec_type:       str
    badge_type:     str
    item_name:      str
    price_raw:      str
    pick_date:      str
    pick_datetime:  str
    slot_label:     str
    slot_remaining: int
    slot_capacity:  int
    sch_idx:        str
    wave_side:      str
    zone_code:      str
    zone_label:     str
    zone_max_cnt:   int
    wave_tags:      list = field(default_factory=list)  # ["M1","M2"] — seasons.json 매핑

    @property
    def available(self) -> bool:
        return self.slot_remaining > 0

    def to_dict(self) -> dict:
        return asdict(self)


# ──────────────────────────────────────────────
# 파서 (동기 — CPU 작업이라 async 불필요)
# ──────────────────────────────────────────────

def parse_item_list(html: str, pick_date: str) -> list[WaveparkItem]:
    items = []
    for card in BeautifulSoup(html, "html.parser").find_all("article", class_="card"):
        try:
            items.append(WaveparkItem(
                pick_date  = card.get("data-pickdate", pick_date),
                item_idx   = card.get("data-itemidx", ""),
                sec_type   = SecType(card.get("data-sectype", "")),
                badge_type = BadgeType(card.find(class_="card-badge-origin").text.strip()),
                name       = card.find(class_="card-name").text.strip(),
                price_raw  = card.find(class_="card-price").text.strip(),
            ))
        except (ValueError, AttributeError) as e:
            log.debug(f"item parse skip: {e}")
    return items


def parse_detail(html: str) -> ItemDetail:
    soup  = BeautifulSoup(html, "html.parser")
    name  = (soup.find(class_="item-name") or soup.new_tag("p")).text.strip()
    desc  = (soup.find(class_="item-desc") or soup.new_tag("p")).text.strip()
    slots = []
    for btn in soup.find_all("button", class_="btnTimeCheck"):
        cap_el = btn.find(class_="opt-cap")
        if cap_el:
            parts     = cap_el.text.strip().split("/")
            remaining = int(parts[0]) if parts[0].isdigit() else 0
            capacity  = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
        else:
            remaining, capacity = 0, 0
        label = btn.get_text(separator=" ", strip=True)
        if cap_el:
            label = label.replace(cap_el.text.strip(), "").strip()
        slots.append(TimeSlot(
            pick_datetime = btn.get("data-pickdatetime", ""),
            item_idx      = btn.get("data-itemidx", ""),
            label         = label,
            remaining     = remaining,
            capacity      = capacity,
        ))
    return ItemDetail(name=name, desc=desc, slots=slots)


def parse_wave_zone(wave_html: str, zone_html: str) -> WaveZoneResult:
    positions = [
        WavePosition(sch_idx=b.get("data-schidx", ""), label=b.text.strip())
        for b in BeautifulSoup(wave_html, "html.parser").find_all("button", class_="btnWaveCheck")
    ]
    zones = [
        SurfZone(
            sch_idx   = b.get("data-schidx", ""),
            wave      = b.get("data-wave", ""),
            zone      = b.get("data-zone", ""),
            max_cnt   = int(b.get("data-maxcnt", 0)),
            item_time = b.get("data-itemtime", ""),
            label     = b.text.strip(),
        )
        for b in BeautifulSoup(zone_html, "html.parser").find_all("button", class_="btnWaveZoneCheck")
    ]
    return WaveZoneResult(positions=positions, zones=zones)


# ──────────────────────────────────────────────
# 비동기 API 클라이언트
# ──────────────────────────────────────────────

class AsyncWaveparkClient:
    def __init__(self, session: aiohttp.ClientSession, sem: asyncio.Semaphore):
        self.session = session
        self.sem     = sem

    async def _post(self, path: str, data: dict) -> dict:
        async with self.sem:                            # 동시 요청 수 제한
            async with self.session.post(
                f"{BASE_URL}{path}", data=data
            ) as resp:
                resp.raise_for_status()
                return await resp.json(content_type=None)

    async def get_list(self, d: date) -> list[WaveparkItem]:
        try:
            data = await self._post("/bookingone/getList", {
                "yy": str(d.year), "mm": str(d.month), "selectYmd": d.isoformat(),
            })
            return parse_item_list(data["html"], d.isoformat()) if data.get("html") else []
        except Exception as e:
            log.warning(f"[{d}] getList 실패: {e}")
            return []

    async def get_detail(self, item: WaveparkItem) -> ItemDetail:
        data = await self._post("/bookingone/getDetail", {
            "pickdate": item.pick_date,
            "secType":  item.sec_type.value,
            "itemIdx":  item.item_idx,
        })
        return parse_detail(data.get("html", ""))

    async def get_wave_zone(self, slot: TimeSlot, no: int = 0) -> WaveZoneResult:
        data = await self._post("/bookingone/getWaveZone", {
            "no": str(no), "itemIdx": slot.item_idx, "pickdatetime": slot.pick_datetime,
        })
        return parse_wave_zone(data.get("waveHtml", ""), data.get("zoneHtml", ""))


# ──────────────────────────────────────────────
# 슬롯 빌더
# ──────────────────────────────────────────────

def _make_zone_slot(
    season: Season,
    item:   WaveparkItem,
    slot:   TimeSlot,
    pos:    WavePosition = None,
    zone:   SurfZone     = None,
) -> ZoneSlot:
    return ZoneSlot(
        season_number  = season.number,
        season_label   = season.label,
        item_idx       = item.item_idx,
        sec_type       = item.sec_type.value,
        badge_type     = item.badge_type.value,
        item_name      = item.name,
        price_raw      = item.price_raw,
        pick_date      = item.pick_date,
        pick_datetime  = slot.pick_datetime,
        slot_label     = slot.label,
        slot_remaining = slot.remaining,
        slot_capacity  = slot.capacity,
        sch_idx        = pos.sch_idx  if pos  else "",
        wave_side      = pos.label    if pos  else "",
        zone_code      = zone.zone    if zone else "",
        zone_label     = zone.label   if zone else "",
        zone_max_cnt   = zone.max_cnt if zone else 0,
        wave_tags      = season.get_wave_tags(item.pick_date, slot.pick_datetime),
    )


# ──────────────────────────────────────────────
# 비동기 수집 단위
# ──────────────────────────────────────────────

async def fetch_slot(
    client:         AsyncWaveparkClient,
    season:         Season,
    item:           WaveparkItem,
    slot:           TimeSlot,
    available_only: bool,
) -> list[ZoneSlot]:
    """슬롯 1개 → getWaveZone → ZoneSlot 리스트"""
    results: list[ZoneSlot] = []

    try:
        wz = await client.get_wave_zone(slot)
    except Exception as e:
        log.warning(f"  getWaveZone 실패 [{item.name} / {slot.label}]: {e}")
        wz = None

    if not wz or not wz.positions:
        entry = _make_zone_slot(season, item, slot)
        if not (available_only and not entry.available):
            results.append(entry)
        return results

    for pos in wz.positions:
        for zone in wz.zones_for(pos.sch_idx):
            entry = _make_zone_slot(season, item, slot, pos, zone)
            if not (available_only and not entry.available):
                results.append(entry)

    return results


async def fetch_item(
    client:         AsyncWaveparkClient,
    season:         Season,
    item:           WaveparkItem,
    target_types:   set[str],
    available_only: bool,
) -> list[ZoneSlot]:
    """상품 1개 → getDetail → 슬롯 병렬 처리"""
    if item.sec_type.value not in target_types:
        return []

    try:
        detail = await client.get_detail(item)
    except Exception as e:
        log.warning(f"  getDetail 실패 [{item.name}]: {e}")
        return []

    if not detail.slots:
        return []

    log.info(
        f"  [{item.sec_type.value}] {item.name} (idx={item.item_idx})"
        f"  슬롯 {len(detail.slots)}개"
    )

    # 슬롯 병렬 처리
    tasks = [
        fetch_slot(client, season, item, slot, available_only)
        for slot in detail.slots
    ]
    nested = await asyncio.gather(*tasks, return_exceptions=False)
    return [entry for sub in nested for entry in sub]


async def fetch_date(
    client:         AsyncWaveparkClient,
    d:              date,
    season:         Season,
    target_types:   set[str],
    available_only: bool,
) -> list[ZoneSlot]:
    """날짜 1개 → getList → 상품 병렬 처리"""
    items = await client.get_list(d)

    # TARGET_ITEMS + secType 필터
    items = [
        it for it in items
        if it.item_idx in TARGET_ITEMS
        and it.sec_type.value in target_types
    ]

    # 같은 날 중복 itemIdx 제거
    seen: set[str] = set()
    unique_items: list[WaveparkItem] = []
    for it in items:
        if it.item_idx not in seen:
            seen.add(it.item_idx)
            unique_items.append(it)

    if not unique_items:
        return []

    log.info(f"▶ {d.isoformat()} ({d.strftime('%a')})  상품 {len(unique_items)}개")

    # 상품 병렬 처리
    tasks = [
        fetch_item(client, season, item, target_types, available_only)
        for item in unique_items
    ]
    nested = await asyncio.gather(*tasks, return_exceptions=False)
    results = [entry for sub in nested for entry in sub]

    if not results:
        log.info(f"  → 데이터 없음")

    return results


# ──────────────────────────────────────────────
# 메인 비동기 스크래퍼
# ──────────────────────────────────────────────

async def scrape_async(
    sec_types:      list[str]     = None,
    available_only: bool          = False,
    force_season:   Optional[int] = None,
    concurrency:    int           = DEFAULT_CONCURRENCY,
    window:         int           = 15,
) -> list[ZoneSlot]:
    seasons      = load_seasons()
    date_season_pairs = resolve_collect_dates(seasons, force_season, window)
    target_types = set(sec_types) if sec_types else {t.value for t in SecType}

    if not date_season_pairs:
        log.warning("수집할 날짜가 없습니다.")
        return []

    d_from = date_season_pairs[0][0]
    d_to   = date_season_pairs[-1][0]
    log.info(
        f"\n{'━'*52}\n"
        f"  수집 범위: {d_from} ~ {d_to}  ({len(date_season_pairs)}일)\n"
        f"  동시 요청 수: {concurrency}\n"
        f"{'━'*52}"
    )

    sem     = asyncio.Semaphore(concurrency)
    timeout = aiohttp.ClientTimeout(total=30)

    t0 = time.perf_counter()

    async with aiohttp.ClientSession(headers=HEADERS, timeout=timeout) as session:
        client = AsyncWaveparkClient(session, sem)

        # 날짜 전체 병렬 처리
        tasks = [
            fetch_date(client, d, season, target_types, available_only)
            for d, season in date_season_pairs
        ]
        nested = await asyncio.gather(*tasks, return_exceptions=False)
        results = [entry for sub in nested for entry in sub]

    elapsed = time.perf_counter() - t0
    log.info(f"\n{'━'*52}")
    log.info(f"수집 완료: 총 {len(results)}개 ZoneSlot  ({elapsed:.1f}초)")
    return results


def scrape(
    sec_types:      list[str]     = None,
    available_only: bool          = False,
    force_season:   Optional[int] = None,
    concurrency:    int           = DEFAULT_CONCURRENCY,
    window:         int           = 15,
) -> list[ZoneSlot]:
    """동기 진입점 — asyncio.run() 래퍼"""
    return asyncio.run(scrape_async(
        sec_types      = sec_types,
        available_only = available_only,
        force_season   = force_season,
        concurrency    = concurrency,
        window         = window,
    ))


# ──────────────────────────────────────────────
# 출력 헬퍼
# ──────────────────────────────────────────────

def print_summary(results: list[ZoneSlot]) -> None:
    if not results:
        print("수집된 데이터가 없습니다.")
        return

    by_season: dict[int, list[ZoneSlot]] = {}
    for r in results:
        by_season.setdefault(r.season_number, []).append(r)

    total_avail = sum(1 for r in results if r.available)
    print(f"\n{'═'*60}")
    print(f"  웨이브파크 스케줄 요약")
    print(f"  총 {len(results)}개 슬롯  (잔여석 있음: {total_avail}개)")
    print(f"{'═'*60}")

    for s_num, s_results in sorted(by_season.items()):
        by_date: dict[str, list[ZoneSlot]] = {}
        for r in s_results:
            by_date.setdefault(r.pick_date, []).append(r)

        print(f"\n  ┌ {s_results[0].season_label}  ({len(by_date)}일)")
        for d_str, slots in sorted(by_date.items()):
            avail_cnt   = sum(1 for s in slots if s.available)
            day_of_week = date.fromisoformat(d_str).strftime("%a")
            print(f"  │")
            print(f"  │  📅 {d_str} ({day_of_week})  잔여 {avail_cnt}/{len(slots)}")

            by_item: dict[str, list[ZoneSlot]] = {}
            for s in slots:
                by_item.setdefault(s.item_name, []).append(s)

            for item_name, item_slots in by_item.items():
                print(f"  │    [{item_slots[0].sec_type}] {item_name}")
                for s in item_slots:
                    mark      = "✓" if s.available else "✗"
                    wave_info = f"{s.wave_side}/{s.zone_label}" if s.wave_side else "—"
                    cnt_info  = f"(max {s.zone_max_cnt})" if s.zone_max_cnt else ""
                    print(
                        f"  │      {mark} {s.slot_label:18s}"
                        f"  잔여 {s.slot_remaining:2d}/{s.slot_capacity}"
                        f"  {wave_info}  {cnt_info}"
                    )
        print(f"  └{'─'*54}")


def save_json(results: list[ZoneSlot], path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump([r.to_dict() for r in results], f, ensure_ascii=False, indent=2)
    log.info(f"JSON 저장: {path}  ({len(results)}개)")


# ──────────────────────────────────────────────
# Supabase upsert
# ──────────────────────────────────────────────

def _difficulty_from_name(item_name: str) -> str:
    """상품명 → 난이도 레이블 (sessions.difficulty 컬럼)"""
    for kw in ("초급", "중급", "상급"):
        if kw in item_name:
            return kw
    return item_name


def _build_sessions_rows(results: list[ZoneSlot], scraped_at: str) -> list[dict]:
    """
    zone_slots 리스트를 프론트엔드 sessions 테이블 rows로 집계합니다.

    집계 키: (pick_date, pick_datetime, item_idx)
    좌/우 잔여석: API에서 per-side 잔여석을 제공하지 않으므로
                 좌/우 zone_max_cnt 비율로 slot_remaining을 배분합니다.
    """
    groups: dict[tuple, list[ZoneSlot]] = defaultdict(list)
    for r in results:
        groups[(r.pick_date, r.pick_datetime, r.item_idx)].append(r)

    rows: list[dict] = []
    for (pick_date, pick_datetime, item_idx), slots in groups.items():
        sample      = slots[0]
        left_slots  = [s for s in slots if s.wave_side == "좌측"]
        right_slots = [s for s in slots if s.wave_side == "우측"]

        left_cap  = sum(s.zone_max_cnt for s in left_slots)
        right_cap = sum(s.zone_max_cnt for s in right_slots)
        total_cap = left_cap + right_cap

        remaining = sample.slot_remaining

        # 좌/우 잔여석 비율 배분 (총 정원이 없으면 동일 값 사용)
        if total_cap:
            left_rem  = round(remaining * left_cap  / total_cap)
            right_rem = round(remaining * right_cap / total_cap)
        else:
            left_rem = right_rem = remaining

        row: dict = {
            "pick_date":       pick_date,
            "pick_datetime":   pick_datetime,
            "item_idx":        item_idx,
            "sec_type":        sample.sec_type,
            "time":            pick_datetime[11:16],          # "09:00"
            "difficulty":      _difficulty_from_name(sample.item_name),
            "wave_tags":       sample.wave_tags,
            "left_remaining":  left_rem,
            "left_capacity":   left_cap or sample.slot_capacity,
            "right_remaining": right_rem,
            "right_capacity":  right_cap or sample.slot_capacity,
            "scraped_at":      scraped_at,
        }

        if sample.sec_type == "70":  # 레슨 블록
            row["lesson_remaining"] = remaining
            row["lesson_capacity"]  = sample.slot_capacity
            if left_slots:
                row["lesson_cove_side"] = "좌측"
            elif right_slots:
                row["lesson_cove_side"] = "우측"

        rows.append(row)

    return rows


def upsert_to_supabase(results: list[ZoneSlot]) -> None:
    """
    수집 결과를 Supabase에 upsert합니다.

    필요 환경변수 (wpbot/.env):
      SUPABASE_URL         — Supabase 프로젝트 URL
      SUPABASE_SERVICE_KEY  — Secret key (구 service_role key, Publishable key는 쓰기 불가)

    대상 테이블:
      zone_slots  — 원시 존 슬롯 데이터
                    unique 제약: (pick_datetime, item_idx, sch_idx, zone_code)
      sessions    — 프론트엔드용 집계 데이터
                    unique 제약: (pick_datetime, item_idx)
    """
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")

    if not url or not key:
        log.error(
            "SUPABASE_URL / SUPABASE_SERVICE_KEY 환경변수 없음. "
            "wpbot/.env 파일을 확인하세요."
        )
        return

    try:
        from supabase import create_client
    except ImportError:
        log.error("supabase 패키지 미설치: pip install supabase python-dotenv")
        return

    client     = create_client(url, key)
    scraped_at = datetime.utcnow().isoformat()
    CHUNK      = 500

    # ── zone_slots ────────────────────────────────────────────
    zone_rows = [{**r.to_dict(), "scraped_at": scraped_at} for r in results]
    log.info(f"[Supabase] zone_slots upsert — {len(zone_rows)}행")
    for i in range(0, len(zone_rows), CHUNK):
        chunk = zone_rows[i : i + CHUNK]
        client.table("zone_slots").upsert(chunk).execute()
        log.info(f"  zone_slots  {min(i + CHUNK, len(zone_rows))}/{len(zone_rows)}")

    # ── sessions ──────────────────────────────────────────────
    session_rows = _build_sessions_rows(results, scraped_at)
    log.info(f"[Supabase] sessions upsert — {len(session_rows)}행")
    for i in range(0, len(session_rows), CHUNK):
        chunk = session_rows[i : i + CHUNK]
        client.table("sessions").upsert(chunk).execute()
        log.info(f"  sessions    {min(i + CHUNK, len(session_rows))}/{len(session_rows)}")

    log.info("[Supabase] 완료")


def print_seasons(seasons: list[Season]) -> None:
    today = date.today()
    print(f"\n{'─'*56}")
    print("  등록된 시즌 목록  (wavepark_seasons.json)")
    print(f"{'─'*56}")
    for s in seasons:
        if s.start <= today <= s.end:
            status = f"🟢 진행중  (종료까지 {s.days_until_end}일)"
        elif today < s.start:
            status = f"🔵 예정    ({(s.start - today).days}일 후 시작)"
        else:
            status = "⚫ 종료"
        print(f"  [{s.number}시즌] {s.label:15s}  {s.start} ~ {s.end}  {status}")
    print(f"{'─'*56}\n")


# ──────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="웨이브파크 시즌 기반 스케줄 스크래퍼 (병렬)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
  python wavepark_scraper.py                          # 현재 시즌 자동 수집
  python wavepark_scraper.py --concurrency 10         # 동시 요청 10개
  python wavepark_scraper.py --sectype 30             # 자유서핑만
  python wavepark_scraper.py --season 2               # 2시즌 강제 수집
  python wavepark_scraper.py --available-only         # 잔여석 있는 것만
  python wavepark_scraper.py --out schedule.json      # JSON 저장
  python wavepark_scraper.py --list-seasons           # 시즌 목록 확인
        """
    )
    parser.add_argument("--sectype",        type=str,  default=None,
                        choices=["30", "70", "90"],
                        help="30=자유서핑 / 70=레슨 / 90=패키지 (미지정=전체)")
    parser.add_argument("--season",         type=int,  default=None,
                        help="특정 시즌 번호 강제 지정")
    parser.add_argument("--concurrency",    type=int,  default=DEFAULT_CONCURRENCY,
                        help=f"동시 요청 수 (기본 {DEFAULT_CONCURRENCY})")
    parser.add_argument("--window",         type=int,  default=15,
                        help="오픈 후 수집할 일수 (기본 15, 오픈 전엔 무시됨)")
    parser.add_argument("--out",            type=str,  default=None,
                        help="결과 JSON 저장 경로")
    parser.add_argument("--available-only", action="store_true",
                        help="잔여석 있는 슬롯만 수집·출력")
    parser.add_argument("--upload",         action="store_true",
                        help="Supabase에 결과 upsert (SUPABASE_URL / SUPABASE_SERVICE_KEY 필요)")
    parser.add_argument("--list-seasons",   action="store_true",
                        help="등록된 시즌 목록 출력 후 종료")
    args = parser.parse_args()

    seasons = load_seasons()

    if args.list_seasons:
        print_seasons(seasons)
        return

    results = scrape(
        sec_types      = [args.sectype] if args.sectype else None,
        available_only = args.available_only,
        force_season   = args.season,
        concurrency    = args.concurrency,
        window         = args.window,
    )

    print_summary(results)

    if args.out:
        save_json(results, args.out)

    if args.upload:
        upsert_to_supabase(results)


if __name__ == "__main__":
    main()
