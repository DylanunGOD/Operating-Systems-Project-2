-- ============================================================================
-- replication_config.sql — Streaming replication PostgreSQL (1 Primary + 2 Replicas)
-- Persona C (FASE 3 / FASE 4).
--
-- Modelo: replicación física por streaming. El Primary acepta escrituras; las
-- réplicas reciben el WAL y sirven lecturas (hot standby). Esto habilita el
-- routing CQRS de src/database/connection.py: write_session -> Primary,
-- read_session -> Réplicas.
--
-- Este archivo documenta y automatiza el lado SQL del setup. Los pasos de
-- sistema de archivos (pg_basebackup, postgresql.conf, pg_hba.conf) van en
-- comentarios porque no se pueden ejecutar desde una sesión SQL.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. EN EL PRIMARY — usuario y slots de replicación
-- ----------------------------------------------------------------------------

-- Rol dedicado a la replicación (mínimo privilegio: solo REPLICATION).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'replicator') THEN
        CREATE ROLE replicator
            WITH REPLICATION LOGIN PASSWORD 'replicator_pwd_change_me';
    END IF;
END
$$;

-- Replication slots: garantizan que el Primary conserve el WAL necesario para
-- cada réplica aunque se desconecte temporalmente (evita perder segmentos).
SELECT pg_create_physical_replication_slot('replica1_slot')
    WHERE NOT EXISTS (
        SELECT 1 FROM pg_replication_slots WHERE slot_name = 'replica1_slot'
    );

SELECT pg_create_physical_replication_slot('replica2_slot')
    WHERE NOT EXISTS (
        SELECT 1 FROM pg_replication_slots WHERE slot_name = 'replica2_slot'
    );

-- ----------------------------------------------------------------------------
-- 2. PARÁMETROS DEL PRIMARY (postgresql.conf o ALTER SYSTEM)
--    Requieren reinicio (wal_level) o reload (los demás).
-- ----------------------------------------------------------------------------
ALTER SYSTEM SET wal_level = 'replica';
ALTER SYSTEM SET max_wal_senders = 10;
ALTER SYSTEM SET max_replication_slots = 10;
ALTER SYSTEM SET wal_keep_size = '512MB';
ALTER SYSTEM SET hot_standby = 'on';
-- synchronous_commit/standby_names: descomentar para replicación SÍNCRONA
-- (cero pérdida de datos a costa de latencia de escritura).
-- ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 (replica1, replica2)';
-- SELECT pg_reload_conf();   -- aplica los cambios que no requieren reinicio

-- ----------------------------------------------------------------------------
-- 3. pg_hba.conf DEL PRIMARY — permitir conexiones de replicación
--    (NO es SQL; agregar estas líneas y recargar)
-- ----------------------------------------------------------------------------
--   # TYPE  DATABASE      USER         ADDRESS            METHOD
--   host    replication   replicator   10.0.0.0/8         scram-sha-256
--   host    replication   replicator   0.0.0.0/0          scram-sha-256   # ajustar en prod

-- ----------------------------------------------------------------------------
-- 4. PROVISIÓN DE CADA RÉPLICA (shell, una sola vez por réplica)
-- ----------------------------------------------------------------------------
--   # En el host de la réplica, con el data dir vacío:
--   pg_basebackup \
--       --host=<PRIMARY_HOST> --port=5432 \
--       --username=replicator \
--       --pgdata=/var/lib/postgresql/data \
--       --wal-method=stream \
--       --write-recovery-conf \
--       --slot=replica1_slot --create-slot  # replica2_slot en la segunda
--
--   # --write-recovery-conf deja primary_conninfo + standby.signal listos.
--   # Verificar en la réplica que postgresql.auto.conf tenga, por ejemplo:
--   #   primary_conninfo = 'host=<PRIMARY_HOST> port=5432 user=replicator
--   #                       password=... application_name=replica1'
--   #   primary_slot_name = 'replica1_slot'
--   # y que exista el archivo standby.signal en el data dir.

-- ----------------------------------------------------------------------------
-- 5. VERIFICACIÓN (correr en el Primary cuando las réplicas estén arriba)
-- ----------------------------------------------------------------------------
-- Estado de cada réplica conectada y su lag de replicación:
--   SELECT application_name, client_addr, state, sync_state,
--          pg_wal_lsn_diff(sent_lsn, replay_lsn) AS replay_lag_bytes
--   FROM pg_stat_replication;
--
-- En cada réplica, confirmar que está en modo recovery (hot standby):
--   SELECT pg_is_in_recovery();   -- debe devolver true
--
-- Lag temporal en la réplica:
--   SELECT now() - pg_last_xact_replay_timestamp() AS replication_delay;
