-- Money Manager -> Supabase schema
-- Run this once in the Supabase SQL Editor.

create table if not exists public.mm_import_batches (
    id bigint generated always as identity primary key,
    file_name text not null,
    file_sha256 text not null unique,
    imported_at timestamptz not null default now(),
    source_transaction_count integer not null default 0,
    source_account_count integer not null default 0,
    source_category_count integer not null default 0,
    source_tag_count integer not null default 0,
    notes text
);

create table if not exists public.mm_accounts (
    source_uid text primary key,
    source_pk integer,
    name text not null,
    nickname text,
    account_type integer,
    group_id integer,
    group_uid text,
    currency_uid text,
    is_deleted boolean not null default false,
    is_reflect boolean not null default false,
    raw jsonb,
    updated_at timestamptz not null default now()
);

create table if not exists public.mm_categories (
    source_uid text primary key,
    source_pk integer,
    name text not null,
    parent_uid text,
    category_type integer,
    is_deleted boolean not null default false,
    raw jsonb,
    updated_at timestamptz not null default now()
);

create table if not exists public.mm_currencies (
    source_uid text primary key,
    source_pk integer,
    iso text,
    main_iso text,
    symbol text,
    rate numeric,
    is_main boolean,
    is_deleted boolean not null default false,
    raw jsonb,
    updated_at timestamptz not null default now()
);

create table if not exists public.mm_transactions (
    source_uid text primary key,
    source_pk integer,
    transaction_date date,
    transaction_datetime timestamptz,
    amount numeric not null default 0,
    amount_account numeric,
    amount_sub numeric,
    account_uid text references public.mm_accounts(source_uid),
    category_uid text references public.mm_categories(source_uid),
    currency_uid text references public.mm_currencies(source_uid),
    to_account_uid text references public.mm_accounts(source_uid),
    opposite_account_id integer,
    transaction_type integer not null,
    transaction_type_name text not null,
    transfer_uid text,
    content text,
    memo text,
    paid text,
    mark text,
    is_deleted boolean not null default false,
    is_projected boolean not null default false,
    source_date_text text,
    import_batch_id bigint references public.mm_import_batches(id),
    raw jsonb,
    updated_at timestamptz not null default now()
);

create index if not exists idx_mm_transactions_date
    on public.mm_transactions(transaction_date);

create index if not exists idx_mm_transactions_account
    on public.mm_transactions(account_uid);

create index if not exists idx_mm_transactions_category
    on public.mm_transactions(category_uid);

create index if not exists idx_mm_transactions_type
    on public.mm_transactions(transaction_type);

create index if not exists idx_mm_transactions_transfer
    on public.mm_transactions(transfer_uid);

create table if not exists public.mm_transaction_tags (
    source_uid text primary key,
    transaction_uid text not null references public.mm_transactions(source_uid) on delete cascade,
    tag_uid text,
    source_pk integer,
    raw jsonb,
    updated_at timestamptz not null default now()
);

create index if not exists idx_mm_transaction_tags_tx
    on public.mm_transaction_tags(transaction_uid);

create table if not exists public.mm_recurring_transactions (
    source_uid text primary key,
    source_pk integer,
    account_uid text,
    to_account_uid text,
    category_uid text,
    currency_uid text,
    transaction_type integer,
    amount numeric,
    next_date date,
    end_date date,
    repeat_type integer,
    memo text,
    payee text,
    is_deleted boolean not null default false,
    raw jsonb,
    updated_at timestamptz not null default now()
);

-- Useful normalized views for the dashboard.
create or replace view public.v_mm_cashflow as
select
    t.source_uid,
    t.transaction_date,
    t.transaction_datetime,
    t.account_uid,
    a.name as account_name,
    t.category_uid,
    c.name as category_name,
    t.amount,
    case
        when t.transaction_type = 0 then 'income'
        when t.transaction_type = 1 then 'expense'
        else 'other'
    end as cashflow_type,
    t.content,
    t.memo,
    t.currency_uid,
    t.is_projected
from public.mm_transactions t
left join public.mm_accounts a on a.source_uid = t.account_uid
left join public.mm_categories c on c.source_uid = t.category_uid
where not t.is_deleted
  and not t.is_projected
  and t.transaction_type in (0,1);

create or replace view public.v_mm_transfers as
select
    t.source_uid,
    t.transfer_uid,
    t.transaction_date,
    t.transaction_datetime,
    t.account_uid,
    a.name as from_account,
    t.to_account_uid,
    ta.name as to_account,
    t.amount,
    t.currency_uid,
    t.is_projected
from public.mm_transactions t
left join public.mm_accounts a on a.source_uid = t.account_uid
left join public.mm_accounts ta on ta.source_uid = t.to_account_uid
where not t.is_deleted
  and t.transaction_type in (3,4)
  and not t.is_projected;

-- Type 7/8 entries are retained separately because Money Manager uses them
-- for balance/difference adjustments, including investment valuation changes.
create or replace view public.v_mm_adjustments as
select
    t.source_uid,
    t.transaction_date,
    t.account_uid,
    a.name as account_name,
    t.amount,
    t.transaction_type,
    t.transaction_type_name,
    t.content,
    t.memo,
    t.is_projected
from public.mm_transactions t
left join public.mm_accounts a on a.source_uid = t.account_uid
where not t.is_deleted
  and t.transaction_type in (7,8)
  and not t.is_projected;

-- Current account ledger balance from recorded transactions.
-- This intentionally does NOT pretend that type 7/8 semantics are ordinary
-- income/expense; the dashboard can show them as adjustments separately.
create or replace view public.v_mm_account_ledger as
select
    a.source_uid,
    a.name as account_name,
    a.currency_uid,
    coalesce(sum(
        case
            when t.transaction_type = 0 then t.amount
            when t.transaction_type = 1 then -t.amount
            when t.transaction_type = 3 then -t.amount
            when t.transaction_type = 4 then t.amount
            when t.transaction_type in (7,8) then t.amount
            else 0
        end
    ),0) as ledger_balance
from public.mm_accounts a
left join public.mm_transactions t
    on t.account_uid = a.source_uid
   and not t.is_deleted
   and not t.is_projected
group by a.source_uid, a.name, a.currency_uid;

-- Private/server-side app: keep RLS enabled and do not grant anon access.
alter table public.mm_import_batches enable row level security;
alter table public.mm_accounts enable row level security;
alter table public.mm_categories enable row level security;
alter table public.mm_currencies enable row level security;
alter table public.mm_transactions enable row level security;
alter table public.mm_transaction_tags enable row level security;
alter table public.mm_recurring_transactions enable row level security;

-- The Streamlit app should use the server-side Supabase key stored in
-- Streamlit secrets. No public anon policy is created here.
