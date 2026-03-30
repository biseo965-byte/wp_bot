"""
웨이브파크 API DTO (Data Transfer Objects)
=========================================
확인된 API 3개 기준으로 정의

  1. GET /bookingone/getList       → WaveparkItem
  2. POST /bookingone/getDetail    → ItemDetail, TimeSlot
  3. POST /bookingone/getWaveZone  → WaveZoneResult, WavePosition, SurfZone
"""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


# ──────────────────────────────────────────────
# 공통 Enum
# ──────────────────────────────────────────────

class SecType(str, Enum):
    """상품 카테고리 코드 (data-sectype)"""
    FREE_SURF = "30"   # 자유서핑 (베이/리프 초·중·상급)
    LESSON    = "70"   # 레슨 (Lv.1~5)
    PACKAGE   = "90"   # 패키지 (주말 리프자유서핑 조합)


class BadgeType(str, Enum):
    """상품 유형 (card-badge-origin)"""
    SINGLE  = "단품"
    PACKAGE = "패키지"


# ──────────────────────────────────────────────
# 1. getList  →  WaveparkItem
# ──────────────────────────────────────────────

@dataclass
class WaveparkItem:
    """
    getList 응답 HTML의 <article class="card"> 1개
    
    source fields:
        data-pickdate        → pick_date
        data-itemidx         → item_idx
        data-sectype         → sec_type
        .card-badge-origin   → badge_type
        .card-name           → name
        .card-price          → price_raw
    """
    pick_date:  str           # "2026-04-26"
    item_idx:   str           # "14", "27809", "30902" ...
    sec_type:   SecType       # SecType.FREE_SURF / LESSON / PACKAGE
    badge_type: BadgeType     # BadgeType.SINGLE / PACKAGE
    name:       str           # "리프자유서핑(중급)"
    price_raw:  str           # "80,000원 ~"  단품은 "~" 포함, 패키지는 없음

    @property
    def price(self) -> Optional[int]:
        """가격 숫자만 추출. 파싱 실패 시 None"""
        digits = "".join(c for c in self.price_raw if c.isdigit())
        return int(digits) if digits else None

    @property
    def is_price_from(self) -> bool:
        """단품처럼 '~' 붙은 최저가 표시 여부"""
        return "~" in self.price_raw


# ──────────────────────────────────────────────
# 2. getDetail  →  TimeSlot, ItemDetail
# ──────────────────────────────────────────────

@dataclass
class TimeSlot:
    """
    getDetail 응답 HTML의 <button class="btnTimeCheck"> 1개

    source fields:
        data-pickdatetime  → pick_datetime
        data-itemidx       → item_idx
        button.ownText()   → label
        span.opt-cap       → "{remaining}/{capacity}"
    
    secType별 특성:
        30 (자유서핑) : 슬롯 복수 (3개+), capacity=50, 1시간 단위
        70 (레슨)     : 슬롯 1개,        capacity=15, 2시간 단위
        90 (패키지)   : 미확인
    """
    pick_datetime: str   # "2026-04-25 12:00:00"
    item_idx:      str   # "14"
    label:         str   # "12:00 ~ 13:00"
    remaining:     int   # 잔여석  (opt-cap 앞 숫자)
    capacity:      int   # 정원    (opt-cap 뒤 숫자)

    @property
    def is_available(self) -> bool:
        return self.remaining > 0

    @property
    def occupancy(self) -> int:
        """현재 점유 인원 = 정원 - 잔여"""
        return self.capacity - self.remaining


@dataclass
class ItemDetail:
    """
    getDetail 응답 HTML 전체

    source fields:
        .item-name          → name
        .item-desc          → desc
        button.btnTimeCheck → slots
        
    hidden form fields (예약 제출용, 파싱 불필요):
        item_time_0, sch_idx_0, item_wavepos_0, item_surfsection_0
    """
    name:  str              # "리프자유서핑(중급)"
    desc:  str              # 현재는 빈 문자열
    slots: list[TimeSlot]   # 타임슬롯 목록


# ──────────────────────────────────────────────
# 3. getWaveZone  →  WavePosition, SurfZone, WaveZoneResult
# ──────────────────────────────────────────────

@dataclass
class WavePosition:
    """
    getWaveZone 응답 waveHtml의 <button class="btnWaveCheck"> 1개

    source fields:
        data-schidx  → sch_idx
        button.text  → label
    """
    sch_idx: str   # "44997"  ← SurfZone.sch_idx 와 1:1 매핑
    label:   str   # "좌측" | "우측"


@dataclass
class SurfZone:
    """
    getWaveZone 응답 zoneHtml의 <button class="btnWaveZoneCheck"> 1개
    파도위치 선택 전엔 display:none 상태

    source fields:
        data-schidx    → sch_idx   (WavePosition.sch_idx 와 연결)
        data-wave      → wave      ("1"=좌측 추정, "2"=우측 추정)
        data-zone      → zone      (구역 코드)
        data-maxcnt    → max_cnt   (해당 구역 최대 정원)
        data-itemtime  → item_time
        button.text    → label
    
    주의:
        같은 타임슬롯이라도 좌/우 max_cnt가 다름
        ex) 좌측 20명, 우측 4명
        현재 점유 인원은 응답에 미포함 → max_cnt 기준으로만 판단 가능
    """
    sch_idx:   str   # "44997"
    wave:      str   # "1" | "2"  (좌=1, 우=2 추정)
    zone:      str   # "20"
    max_cnt:   int   # 최대 정원
    item_time: str   # "2026-04-25 12:00:00"
    label:     str   # "중급리프"

    @property
    def wave_side(self) -> str:
        """wave 값을 사람이 읽을 수 있는 텍스트로 변환 (추정값)"""
        return {"1": "좌측", "2": "우측"}.get(self.wave, f"unknown({self.wave})")


@dataclass
class WaveZoneResult:
    """
    getWaveZone 전체 응답

    waveHtml → positions (파도위치 목록)
    zoneHtml → zones     (서핑구역 목록, 전체 포함)

    사용법:
        result.zones_for("44997")  →  좌측에 해당하는 구역만 필터링
    """
    positions: list[WavePosition]
    zones:     list[SurfZone]

    def zones_for(self, sch_idx: str) -> list[SurfZone]:
        """특정 파도위치(sch_idx)에 해당하는 서핑구역 반환"""
        return [z for z in self.zones if z.sch_idx == sch_idx]

    def zones_for_position(self, position: WavePosition) -> list[SurfZone]:
        """WavePosition 객체로 직접 조회"""
        return self.zones_for(position.sch_idx)


# ──────────────────────────────────────────────
# 전체 예약 선택 상태를 담는 복합 DTO
# ──────────────────────────────────────────────

@dataclass
class BookingSelection:
    """
    3단계 API 호출 결과를 하나로 묶은 예약 선택 상태
    웨일 앱에서 세션 뷰어로 활용할 때 사용

    usage:
        selection = BookingSelection(
            item    = WaveparkItem(...),
            detail  = ItemDetail(...),
            slot    = TimeSlot(...),          # 선택된 슬롯
            wave    = WaveZoneResult(...),
            position = WavePosition(...),     # 선택된 파도위치
            zone     = SurfZone(...),         # 선택된 서핑구역
        )
    """
    item:     WaveparkItem
    detail:   ItemDetail
    slot:     Optional[TimeSlot]      = None   # 타임슬롯 선택 후 세팅
    wave:     Optional[WaveZoneResult]= None   # getWaveZone 호출 후 세팅
    position: Optional[WavePosition]  = None   # 파도위치 선택 후 세팅
    zone:     Optional[SurfZone]      = None   # 서핑구역 선택 후 세팅

    @property
    def is_complete(self) -> bool:
        """예약 제출 가능 상태인지 확인"""
        return all([self.slot, self.position, self.zone])

    def summary(self) -> str:
        parts = [
            f"상품    : {self.item.name}  ({self.item.price_raw})",
            f"날짜    : {self.slot.pick_datetime if self.slot else '-'}",
            f"시간    : {self.slot.label if self.slot else '-'}",
            f"잔여석  : {self.slot.remaining}/{self.slot.capacity if self.slot else '-'}",
            f"파도위치: {self.position.label if self.position else '-'}",
            f"서핑구역: {self.zone.label if self.zone else '-'} (최대 {self.zone.max_cnt}명)" if self.zone else "서핑구역: -",
        ]
        return "\n".join(parts)
