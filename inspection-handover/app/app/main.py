"""FastAPI 入口：工单提交/查询、交接、字段一次性发布、订阅暂停恢复，以及演示网页。"""
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

from . import db
from .config import get_settings
from .cutover import CutoverError, CutoverManager
from .rollout import RolloutError, RolloutManager
from .state import StateStore

settings = get_settings()
store = StateStore(settings.state_db_path)
manager = CutoverManager(settings, store)
rollout = RolloutManager(settings, store)

app = FastAPI(title="巡检工单库交接", version="1.1")


class WorkOrderIn(BaseModel):
    title: str = Field(min_length=1, max_length=200)
    detail: str = Field(default="", max_length=2000)
    # 优先级 0~3（Field 约束，越界返回 422）；字段发布完成前网页不提交该字段，
    # 接口收到非空值也会以 409 拒绝，见 submit_work_order
    priority: int | None = Field(default=None, ge=0, le=3)


class CutoverRequest(BaseModel):
    confirm: bool = False


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return PAGE_HTML


@app.get("/api/status")
def status() -> dict:
    state = manager.current_state()
    accepting = manager.accepting_db()
    sub = manager.subscription_overview()
    return {
        "state": state,
        "accepting_db": accepting,
        "inflight_writes": manager.inflight,
        "subscription": sub,
        "priority_rollout": rollout.overview(),
    }


@app.get("/api/work-orders")
def work_orders() -> dict:
    # 两库数据都展示，source_db 标明每条工单实际由哪个库接单
    return {
        "accepting_db": manager.accepting_db(),
        "old": db.list_work_orders(settings.old_dsn),
        "new": db.list_work_orders(settings.new_dsn),
        "old_summary": {"count_max": db.count_and_max(settings.old_dsn)},
        "new_summary": {"count_max": db.count_and_max(settings.new_dsn)},
    }


@app.post("/api/work-orders")
def submit_work_order(payload: WorkOrderIn) -> dict:
    state = manager.current_state()
    if state == "SWITCHING":
        raise HTTPException(status_code=503, detail="交接进行中，旧库已暂停接单，请稍后再试")
    # priority 仅在一次性发布完成后才受理；未完成期间一律走旧写法（priority 置空），
    # 避免把只有单边库具备的列写入复制链路。
    if payload.priority is not None and not rollout.is_completed():
        raise HTTPException(
            status_code=409,
            detail="priority 字段尚未发布完成，请先完成字段发布或暂不填写优先级",
        )
    if state == "NEW":
        target_dsn, target_name = settings.new_dsn, "new"
    else:
        target_dsn, target_name = settings.old_dsn, "old"

    manager.enter_write()
    try:
        row = db.insert_work_order(
            target_dsn, payload.title, payload.detail, target_name, payload.priority
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"写入失败: {exc}") from exc
    finally:
        manager.exit_write()
    return {
        "work_order": row,
        "accepted_by": target_name,
        "state": state,
        "priority_accepted": rollout.is_completed(),
    }


@app.post("/api/cutover")
def start_cutover(payload: CutoverRequest) -> JSONResponse:
    if not payload.confirm:
        raise HTTPException(status_code=400, detail="需要 confirm=true 确认发起单向交接")
    try:
        result = manager.start_cutover()
    except CutoverError as exc:
        message = str(exc)
        status_code = 504 if "超时" in message else 500
        # 出错/超时已回滚，返回当前状态（应为 OLD）
        return JSONResponse(
            status_code=status_code,
            content={
                "ok": False,
                "error": message,
                "state": manager.current_state(),
                "accepting_db": manager.accepting_db(),
            },
        )
    return JSONResponse(content={"ok": True, **result})


@app.get("/api/priority-rollout")
def priority_rollout_status() -> dict:
    return rollout.overview()


@app.post("/api/priority-rollout")
def start_priority_rollout() -> JSONResponse:
    """一次性字段发布（可重试）：先订阅端后发布端增加可空 priority 列。

    幂等：已完成返回 already_done；部分完成时从持久化阶段续跑，已生效的
    DDL 与追平步骤不重复执行。失败保持旧写法，响应带回未完成的库。
    """
    try:
        result = rollout.start_rollout()
    except RolloutError as exc:
        message = str(exc)
        status_code = 409 if "进行中" in message else (
            504 if "超时" in message else 500
        )
        return JSONResponse(
            status_code=status_code,
            content={"ok": False, "error": message, **rollout.overview()},
        )
    return JSONResponse(content={"ok": True, **result})


@app.post("/api/subscription/pause")
def pause_subscription() -> dict:
    try:
        return manager.pause_subscription()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"暂停订阅失败: {exc}") from exc


@app.post("/api/subscription/resume")
def resume_subscription() -> dict:
    try:
        return manager.resume_subscription()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"恢复订阅失败: {exc}") from exc


@app.get("/api/events")
def events() -> list:
    return store.list_events()


PAGE_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>巡检工单库交接</title>
<style>
  body { font-family: system-ui, "PingFang SC", "Microsoft YaHei", sans-serif; margin: 24px; background: #f6f7f9; color: #1f2933; }
  h1 { font-size: 20px; } h2 { font-size: 16px; margin-top: 28px; }
  .card { background: #fff; border: 1px solid #e4e7eb; border-radius: 8px; padding: 16px; margin-bottom: 16px; }
  table { border-collapse: collapse; width: 100%; font-size: 13px; }
  th, td { border: 1px solid #e4e7eb; padding: 6px 8px; text-align: left; }
  th { background: #f0f2f5; }
  button { padding: 8px 14px; border: 0; border-radius: 6px; cursor: pointer; margin-right: 8px; }
  .primary { background: #2563eb; color: #fff; } .warn { background: #d97706; color: #fff; }
  .ok { background: #059669; color: #fff; } .muted { background: #e4e7eb; color: #334155; }
  .badge { display: inline-block; padding: 2px 10px; border-radius: 999px; font-size: 12px; font-weight: 600; }
  .b-old { background: #dbeafe; color: #1d4ed8; } .b-new { background: #d1fae5; color: #047857; }
  .b-switching { background: #fef3c7; color: #b45309; }
  input, textarea { width: 100%; padding: 8px; margin: 4px 0 10px; border: 1px solid #cbd2d9; border-radius: 6px; box-sizing: border-box; }
  pre { background: #0f172a; color: #e2e8f0; padding: 12px; border-radius: 8px; font-size: 12px; overflow:auto; max-height: 260px; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
  #msg { margin: 8px 0; font-size: 13px; white-space: pre-wrap; }
</style>
</head>
<body>
<h1>巡检工单库交接演示（PostgreSQL 逻辑复制）</h1>

<div class="card">
  <strong>当前状态：</strong><span id="state" class="badge b-old">-</span>
  &nbsp;接单库：<span id="accepting" class="badge b-old">-</span>
  &nbsp;在途写入：<span id="inflight">-</span>
  &nbsp;订阅：<span id="substate">-</span>
  <div style="margin-top:10px">
    <button class="muted" onclick="refresh()">刷新状态</button>
    <button class="primary" onclick="startCutover()">发起单向交接（旧库 → 新库）</button>
    <button class="warn" onclick="pauseSub()">暂停订阅（制造追平延迟）</button>
    <button class="ok" onclick="resumeSub()">恢复订阅</button>
  </div>
  <div id="msg"></div>
</div>

<div class="card">
  <h2 style="margin-top:0">priority 字段一次性发布（逻辑复制不同步 DDL：先订阅端、后发布端）</h2>
  <div>
    发布阶段：<strong id="rollout-phase">-</strong>
    &nbsp;|&nbsp; 新库（订阅端）列：<span id="rollout-newcol">-</span>
    &nbsp;|&nbsp; 旧库（发布端）列：<span id="rollout-oldcol">-</span>
    &nbsp;|&nbsp; 未完成的库：<span id="rollout-unfinished">-</span>
  </div>
  <div style="margin-top:10px">
    <button class="primary" id="rollout-btn" onclick="startRollout()">开始/重试字段发布</button>
    <small>失败时工单仍按旧写法提交；重复点击与重启均从已生效阶段续跑</small>
  </div>
  <div id="rolloutmsg"></div>
</div>

<div class="card">
  <h2 style="margin-top:0">提交巡检工单（统一入口，自动发往当前接单库）</h2>
  <label>标题</label><input id="title" placeholder="例如：3号机组例行巡检">
  <label>详情</label><textarea id="detail" rows="2"></textarea>
  <label>优先级（0~3，字段发布完成后开放；不填为 NULL）</label>
  <select id="priority" disabled style="width:100%;padding:8px;margin:4px 0 10px;border:1px solid #cbd2d9;border-radius:6px;box-sizing:border-box">
    <option value="">（不设置 / NULL）</option>
    <option value="0">0 - 最低</option>
    <option value="1">1</option>
    <option value="2">2</option>
    <option value="3">3 - 最高</option>
  </select>
  <button class="primary" onclick="submitOrder()">提交工单</button>
  <div id="submitmsg"></div>
</div>

<div class="grid">
  <div class="card">
    <h2 style="margin-top:0">旧库工单（发布端） <small id="oldsum"></small></h2>
    <table><thead><tr><th>编号</th><th>标题</th><th>优先级</th><th>来源</th><th>时间</th></tr></thead>
    <tbody id="oldrows"></tbody></table>
  </div>
  <div class="card">
    <h2 style="margin-top:0">新库工单（订阅端） <small id="newsum"></small></h2>
    <table><thead><tr><th>编号</th><th>标题</th><th>优先级</th><th>来源</th><th>时间</th></tr></thead>
    <tbody id="newrows"></tbody></table>
  </div>
</div>

<div class="card">
  <h2 style="margin-top:0">交接事件日志</h2>
  <pre id="events"></pre>
</div>

<script>
function badgeClass(v){ return v === 'NEW' ? 'b-new' : v === 'SWITCHING' ? 'b-switching' : 'b-old'; }
function show(el, text, color){ el.textContent = text; el.style.color = color || '#334155'; }

async function refresh(){
  const s = await (await fetch('/api/status')).json();
  const st = document.getElementById('state');
  st.textContent = s.state; st.className = 'badge ' + badgeClass(s.state);
  const ac = document.getElementById('accepting');
  ac.textContent = s.accepting_db; ac.className = 'badge ' + badgeClass(s.accepting_db === 'new' ? 'NEW' : 'OLD');
  document.getElementById('inflight').textContent = s.inflight_writes;
  const sub = s.subscription.subscriber;
  document.getElementById('substate').textContent =
    sub.exists ? (sub.enabled ? '启用中' : '已暂停') : '不存在';

  const w = await (await fetch('/api/work-orders')).json();
  fillRows('oldrows', w.old); fillRows('newrows', w.new);
  document.getElementById('oldsum').textContent = '行数/最大号: ' + w.old_summary.count_max.join(' / ');
  document.getElementById('newsum').textContent = '行数/最大号: ' + w.new_summary.count_max.join(' / ');

  // priority 字段发布状态：完成前禁用优先级下拉框
  const r = s.priority_rollout;
  document.getElementById('rollout-phase').textContent =
    r.phase + '（' + r.phase_label + '）';
  document.getElementById('rollout-newcol').textContent = r.new_db_has_column ? '已具备' : '无';
  document.getElementById('rollout-oldcol').textContent = r.old_db_has_column ? '已具备' : '无';
  document.getElementById('rollout-unfinished').textContent =
    r.unfinished_dbs.length ? r.unfinished_dbs.join(', ') : '（无）';
  const priSel = document.getElementById('priority');
  priSel.disabled = !r.completed;
  document.getElementById('rollout-btn').textContent =
    r.completed ? '字段已发布（重复点击仅回显）'
      : (r.in_progress ? '继续/重试字段发布' : '开始/重试字段发布');
  if (r.last_error) {
    show(document.getElementById('rolloutmsg'),
      '上次未完成：' + r.last_error.message + '（阶段 ' + r.last_error.phase + '）', '#b91c1c');
  }

  const ev = await (await fetch('/api/events')).json();
  document.getElementById('events').textContent =
    ev.map(e => new Date(e.ts*1000).toLocaleTimeString() + ' [' + e.phase + '] ' + e.message).join('\\n') || '（暂无）';
}

function fillRows(id, rows){
  document.getElementById(id).innerHTML = rows.map(r =>
    '<tr><td>' + r.id + '</td><td>' + escapeHtml(r.title) + '</td><td>' +
    (r.priority === null || r.priority === undefined ? '—' : r.priority) +
    '</td><td>' + r.source_db +
    '</td><td>' + new Date(r.created_at).toLocaleString() + '</td></tr>').join('') ||
    '<tr><td colspan="5">（无）</td></tr>';
}
function escapeHtml(s){ return (s||'').replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c])); }

async function submitOrder(){
  const body = {
    title: document.getElementById('title').value.trim(),
    detail: document.getElementById('detail').value
  };
  const priVal = document.getElementById('priority').value;
  if(priVal !== '') body.priority = Number(priVal);
  if(!body.title){ show(document.getElementById('submitmsg'), '请填写标题', '#b91c1c'); return; }
  const r = await fetch('/api/work-orders', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
  const d = await r.json();
  if(r.ok){
    show(document.getElementById('submitmsg'),
      '已受理：编号 ' + d.work_order.id + '，接单库：' + d.accepted_by +
      '，优先级：' + (d.work_order.priority === null ? 'NULL' : d.work_order.priority), '#047857');
    document.getElementById('title').value = ''; document.getElementById('detail').value = '';
    document.getElementById('priority').value = '';
    refresh();
  } else {
    show(document.getElementById('submitmsg'), '提交失败：' + (d.detail || r.status), '#b91c1c');
  }
}

async function startRollout(){
  const r = await fetch('/api/priority-rollout', { method:'POST' });
  const d = await r.json();
  const el = document.getElementById('rolloutmsg');
  if(r.ok && d.ok){
    show(el, d.already_done ? '字段发布早已完成，重复点击不重复执行'
      : '发布成功：两库均已具备可空 priority(0~3) 列，优先级填写已开放', '#047857');
  } else {
    show(el, '发布未完成，工单仍按旧写法提交：' + (d.error || r.status) +
      '；未完成的库：' + ((d.unfinished_dbs && d.unfinished_dbs.length) ? d.unfinished_dbs.join(', ') : '无'), '#b91c1c');
  }
  refresh();
}

async function startCutover(){
  if(!confirm('确认发起单向交接？旧库将暂停接单，追平并对齐序列后由新库接单。')) return;
  const r = await fetch('/api/cutover', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({confirm:true}) });
  const d = await r.json();
  const el = document.getElementById('msg');
  if(r.ok && d.ok){
    show(el, d.already_done ? '交接早已生效，当前接单库：新库（重复点击不重复切换）'
      : '交接成功！首个新编号：' + d.first_new_id + '，旧库最高预留号：' + d.old_highest_reserved, '#047857');
  } else {
    show(el, '交接失败已回滚，旧库恢复接单：' + d.error, '#b91c1c');
  }
  refresh();
}

async function pauseSub(){
  const r = await fetch('/api/subscription/pause', { method:'POST' });
  show(document.getElementById('msg'), r.ok ? '订阅已暂停，可继续在旧库写工单制造延迟，再发起交接观察超时' : '操作失败', r.ok ? '#b45309' : '#b91c1c');
  refresh();
}
async function resumeSub(){
  const r = await fetch('/api/subscription/resume', { method:'POST' });
  show(document.getElementById('msg'), r.ok ? '订阅已恢复，积压数据将开始追平' : '操作失败', r.ok ? '#047857' : '#b91c1c');
  refresh();
}
refresh(); setInterval(refresh, 4000);
</script>
</body>
</html>
"""
