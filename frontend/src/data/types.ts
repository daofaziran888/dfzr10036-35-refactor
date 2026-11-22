/** 纯前端数据层的公共类型：与全栈版后端返回的结构保持一致，换回后端时页面不用改。 */

export type EntryRow = {
  id: number
  status: string
  pending: boolean
  abnormal: boolean
  [field: string]: string | number | boolean
}

export type ModuleMeta = {
  key: string
  name: string
  entity: string
  desc: string
  fields: string[]
  statuses: string[]
  actions: string[]
  actionTargets: Record<string, string>
  metrics: string[]
}

export type PageResult = {
  items: EntryRow[]
  total: number
  page: number
  size: number
}

export type ActionEntry = {
  method: string
  path: string
  params?: Record<string, string | number>
  body?: Record<string, unknown>
  说明?: string
}

export type StartupReceipt = {
  state: 'started' | 'reused' | 'pending'
  受理结果: string
  登记编号: string | null
  task_id: number
  任务编号?: string
  本侧: string
  confirmed_by?: string | null
  确认方?: string | null
  确认入口?: ActionEntry | null
  未写入方?: string[]
  not_written_by?: string[]
  续办入口?: ActionEntry | null
  resume_entry?: ActionEntry | null
  说明?: string | null
}

export type StartupState = {
  task: EntryRow
  registration: Record<string, unknown> | null
  start_state: 'none' | 'pending' | 'started' | 'reused'
  confirmed_by: string | null
  confirm_entry: ActionEntry | null
  not_written_by: string[]
  resume_entry: ActionEntry | null
}

export type ActionResult = {
  ok: boolean
  message: string
  receipt?: StartupReceipt | null
  start_state?: string | null
}

export type OverviewResult = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}
