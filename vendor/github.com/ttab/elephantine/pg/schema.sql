CREATE TABLE public.job_lock (
    name text NOT NULL,
    holder text NOT NULL,
    touched timestamp with time zone NOT NULL,
    iteration bigint NOT NULL
);

