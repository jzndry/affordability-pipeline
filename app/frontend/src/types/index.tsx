export type TabType = 'overview' | 'sandbox' | 'showcase';

export interface Transaction {
  id: string;
  amount: number;
  date: string;
  raw_description: string;
}

export interface BankStatementPayload {
  statement_id: string;
  account_holder: string;
  account_number: string;
  sort_code: string;
  currency: string;
  transactions: Transaction[];
}

export interface AssessmentMetrics {
  total_income?: number;
  monthly_net_income?: number;
  net_income?: number;
  essential_outgoings?: number;
  total_outgoings?: number;
  outgoings?: number;
  debt_to_income_ratio?: number;
  dti_ratio?: number;
  dti?: number;
  gambling_percentage?: number;
  gambling_ratio?: number;
  gambling_spend_ratio?: number;
}

export interface UnderwritingResult {
  verdict?: string;
  status?: string;
  decision?: string;
  metrics?: AssessmentMetrics;
  [key: string]: unknown;
}

export interface WebSocketEventPayload {
  event?: string;
  job_id?: string;
  message?: string;
  data?: UnderwritingResult;
  result?: UnderwritingResult;
  [key: string]: unknown;
}

export interface IngestionApiResponse {
  message: string;
  job_id: string;
  status: string;
}

export interface LogEntry {
  time: string;
  text: string;
  type: 'info' | 'success' | 'error' | 'warn';
}