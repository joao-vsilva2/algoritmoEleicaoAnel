import socket
import json
import time
import logging
import os
import io
import math
import multiprocessing

BASE_PORT = 8000
HOST = "127.0.0.1"
LOG_COLLECTOR_PORT = 8999
NOS = [10, 20, 30, 40, 90, 60, 70, 80]


class MessageType:
    ELECTION = "ELECTION"
    COORDINATOR = "COORDINATOR"
    START_ELECTION = "START_ELECTION"
    STATUS_REQ = "STATUS_REQ"


def send_tcp_message(target_port, payload, timeout=0.6, expect_reply=False):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        sock.connect((HOST, target_port))
        data = (json.dumps(payload) + "\n").encode("utf-8")
        sock.sendall(data)
        if expect_reply:
            response = b""
            while not response.endswith(b"\n"):
                chunk = sock.recv(1024)
                if not chunk:
                    break
                response += chunk
            if response:
                return json.loads(response.decode("utf-8").strip())
    return None


class RingNodeProcess(multiprocessing.Process):
    """Processo independente do SO que se comunica exclusivamente via Sockets TCP."""

    def __init__(self, process_id, all_ids):
        super().__init__()
        self.process_id = process_id
        self.all_ids = sorted(all_ids)
        self.port = BASE_PORT + process_id
        self.coordinator = None
        self.in_election = False

        idx = self.all_ids.index(self.process_id)
        self.successor_id = self.all_ids[(idx + 1) % len(self.all_ids)]

    def log(self, level, message):
        try:
            send_tcp_message(
                LOG_COLLECTOR_PORT,
                {"level": level, "message": message},
                timeout=0.5,
                expect_reply=False
            )
        except Exception:
            pass

    def run(self):
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((HOST, self.port))
        server_sock.listen(16)
        server_sock.settimeout(0.5)

        try:
            while True:
                try:
                    conn, _ = server_sock.accept()
                except socket.timeout:
                    continue

                with conn:
                    conn.settimeout(1.0)
                    raw = b""
                    while not raw.endswith(b"\n"):
                        chunk = conn.recv(1024)
                        if not chunk:
                            break
                        raw += chunk

                    if not raw:
                        continue

                    msg = json.loads(raw.decode("utf-8").strip())
                    reply = self.handle_message(msg)
                    if reply is not None:
                        conn.sendall((json.dumps(reply) + "\n").encode("utf-8"))
        finally:
            server_sock.close()

    def handle_message(self, msg):
        m_type = msg.get("type")

        if m_type == MessageType.STATUS_REQ:
            return {
                "process_id": self.process_id,
                "coordinator": self.coordinator,
                "successor_id": self.successor_id,
                "in_election": self.in_election
            }

        elif m_type == MessageType.START_ELECTION:
            self.start_election()
            return None

        elif m_type == MessageType.ELECTION:
            origin_id = msg.get("origin_id")
            self.log("INFO", f"[P{self.process_id}] Recebeu ELECTION(id={origin_id})")

            if origin_id > self.process_id:
                self.in_election = True
                self.forward_to_successor(msg)
            elif origin_id < self.process_id:
                self.log(
                    "INFO",
                    f"[P{self.process_id}] ID menor que {origin_id} -> descartando mensagem."
                )
            elif origin_id == self.process_id:
                self.log(
                    "INFO",
                    f"[P{self.process_id}] ID completou a volta no anel -> eleito NOVO COORDENADOR."
                )
                self.in_election = False
                self.announce_coordinator()

        elif m_type == MessageType.COORDINATOR:
            coord_id = msg.get("coordinator_id")
            if self.coordinator != coord_id:
                self.coordinator = coord_id
                self.in_election = False
                self.log(
                    "INFO",
                    f"[P{self.process_id}] Reconheceu P{coord_id} como Coordenador oficial."
                )
                if coord_id != self.process_id:
                    self.forward_to_successor(msg)

        return None

    def start_election(self):
        self.in_election = True
        msg = {
            "type": MessageType.ELECTION,
            "origin_id": self.process_id
        }
        self.log(
            "INFO",
            f"[P{self.process_id}] Iniciou eleicao (enviando ID {self.process_id} para P{self.successor_id})."
        )
        self.forward_to_successor(msg)

    def announce_coordinator(self):
        self.coordinator = self.process_id
        msg = {
            "type": MessageType.COORDINATOR,
            "coordinator_id": self.process_id
        }
        self.log(
            "INFO",
            f"[P{self.process_id}] Anunciando vitoria do Coordenador P{self.process_id} no anel."
        )
        self.forward_to_successor(msg)

    def forward_to_successor(self, msg):
        n = len(self.all_ids)
        curr_target_id = self.successor_id
        attempts = 0

        while attempts < n:
            if curr_target_id == self.process_id:
                self.log("ERROR", f"[P{self.process_id}] Nenhum outro processo ativo encontrado no anel!")
                return

            target_port = BASE_PORT + curr_target_id
            try:
                time.sleep(0.05)
                send_tcp_message(target_port, msg, timeout=0.6, expect_reply=False)

                if curr_target_id != self.successor_id:
                    self.log(
                        "INFO",
                        f"[Reconfiguracao] P{self.process_id} atualizou sucessor direto para P{curr_target_id}."
                    )
                    self.successor_id = curr_target_id

                self.log(
                    "INFO",
                    f"[P{self.process_id}] Encaminhou {msg.get('type')} -> P{curr_target_id}"
                )
                return

            except (socket.timeout, ConnectionRefusedError, OSError):
                self.log(
                    "WARNING",
                    f"[P{self.process_id}] Falha/Timeout ao contatar P{curr_target_id} -> pulando vizinho..."
                )
                idx = self.all_ids.index(curr_target_id)
                curr_target_id = self.all_ids[(idx + 1) % n]
                attempts += 1


class LogCollectorServer(multiprocessing.Process):
    """Servidor TCP auxiliar que centraliza os logs dos processos de forma organizada."""

    def __init__(self, log_file_path):
        super().__init__()
        self.log_file_path = log_file_path
        self.stop_event = multiprocessing.Event()

    def run(self):
        os.makedirs(os.path.dirname(self.log_file_path), exist_ok=True)
        log_buffer = io.StringIO()

        logger = logging.getLogger("CentralCollector")
        logger.setLevel(logging.INFO)
        logger.handlers.clear()

        formatter = logging.Formatter("%(asctime)s.%(msecs)03d | %(levelname)-7s | %(message)s", datefmt="%H:%M:%S")

        stream_h = logging.StreamHandler()
        stream_h.setFormatter(formatter)

        mem_h = logging.StreamHandler(log_buffer)
        mem_h.setFormatter(formatter)

        logger.addHandler(stream_h)
        logger.addHandler(mem_h)

        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((HOST, LOG_COLLECTOR_PORT))
        server.listen(32)
        server.settimeout(0.3)

        try:
            while not self.stop_event.is_set():
                try:
                    conn, _ = server.accept()
                except socket.timeout:
                    continue

                with conn:
                    raw = b""
                    while not raw.endswith(b"\n"):
                        chunk = conn.recv(1024)
                        if not chunk:
                            break
                        raw += chunk
                    if raw:
                        entry = json.loads(raw.decode("utf-8").strip())
                        lvl = entry.get("level", "INFO")
                        msg = entry.get("message", "")

                        if not msg.strip():
                            continue

                        if msg.startswith("--- ") and msg.endswith(" ---"):
                            log_buffer.write(f"\n{msg}\n")
                            logger.info(msg)
                            continue

                        if lvl == "WARNING":
                            logger.warning(msg)
                        elif lvl == "ERROR":
                            logger.error(msg)
                        else:
                            logger.info(msg)
        finally:
            server.close()
            with open(self.log_file_path, "w", encoding="utf-8") as f:
                f.write(log_buffer.getvalue())

    def shutdown(self):
        self.stop_event.set()


class RingController:
    def __init__(self, ids=NOS):
        self.ids = sorted(ids)
        self.processes = {}
        for pid in self.ids:
            self.processes[pid] = RingNodeProcess(pid, self.ids)

    def start_all(self):
        for p in self.processes.values():
            p.start()
            time.sleep(0.05)
        time.sleep(0.8)
        self.send_log("INFO", f"Anel iniciado via TCP com {len(self.ids)} processos: {self.ids}")

    def send_log(self, level, message):
        try:
            send_tcp_message(LOG_COLLECTOR_PORT, {"level": level, "message": message}, timeout=0.5)
        except Exception:
            pass

    def trigger_election(self, pid):
        port = BASE_PORT + pid
        send_tcp_message(port, {"type": MessageType.START_ELECTION}, timeout=0.6)

    def kill_process(self, pid):
        proc = self.processes.get(pid)
        if proc and proc.is_alive():
            proc.terminate()
            proc.join(timeout=1.0)

    def query_ring_state(self):
        states = {}
        for pid in self.ids:
            proc = self.processes[pid]
            if not proc.is_alive():
                states[pid] = {
                    "is_active": False,
                    "coordinator": None,
                    "successor_id": None,
                    "in_election": False
                }
                continue

            try:
                resp = send_tcp_message(
                    BASE_PORT + pid,
                    {"type": MessageType.STATUS_REQ},
                    timeout=1.5,
                    expect_reply=True
                )
                states[pid] = {
                    "is_active": True,
                    "coordinator": resp.get("coordinator"),
                    "successor_id": resp.get("successor_id"),
                    "in_election": resp.get("in_election")
                }
            except Exception:
                states[pid] = {
                    "is_active": False,
                    "coordinator": None,
                    "successor_id": None,
                    "in_election": False
                }
        return states

    def stop_all(self):
        for p in self.processes.values():
            if p.is_alive():
                p.terminate()
                p.join(timeout=0.5)
        self.send_log("INFO", "Todos os processos da simulacao foram encerrados.")


def generate_svg_stage(ring_ctrl, stage_num, stage_title, filename):
    os.makedirs("representacao", exist_ok=True)
    filepath = os.path.join("representacao", filename)

    states = ring_ctrl.query_ring_state()
    ids = ring_ctrl.ids
    n = len(ids)

    width, height = 650, 680
    cx, cy, radius = 325, 310, 190

    svg_lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" width="100%" height="100%">',
        '  <defs>',
        '    <marker id="arrow" viewBox="0 0 10 10" refX="22" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">',
        '      <path d="M 0 0 L 10 5 L 0 10 z" fill="#4B5563" />',
        '    </marker>',
        '    <marker id="arrow-active" viewBox="0 0 10 10" refX="22" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">',
        '      <path d="M 0 0 L 10 5 L 0 10 z" fill="#2563EB" />',
        '    </marker>',
        '    <filter id="shadow" x="-20%" y="-20%" width="140%" height="140%">',
        '      <feDropShadow dx="2" dy="4" stdDeviation="3" flood-opacity="0.15" />',
        '    </filter>',
        '  </defs>',
        '  <rect width="100%" height="100%" fill="#F8FAFC" rx="16" />',
        f'  <text x="{cx}" y="40" font-family="system-ui, sans-serif" font-size="20" font-weight="bold" text-anchor="middle" fill="#1E293B">Estagio {stage_num}: {stage_title}</text>',
    ]

    pos = {}
    for i, pid in enumerate(ids):
        angle = 2 * math.pi * i / n - math.pi / 2
        px = cx + radius * math.cos(angle)
        py = cy + radius * math.sin(angle)
        pos[pid] = (px, py)

    for pid in ids:
        st = states[pid]
        if not st["is_active"]:
            continue

        succ_id = st["successor_id"]
        visited = 0
        while succ_id and not states[succ_id]["is_active"] and visited < n:
            idx = ids.index(succ_id)
            succ_id = ids[(idx + 1) % n]
            visited += 1

        if succ_id and states[succ_id]["is_active"]:
            x1, y1 = pos[pid]
            x2, y2 = pos[succ_id]
            is_election_path = (stage_num in [1, 4])
            stroke_color = "#2563EB" if is_election_path else "#94A3B8"
            marker_id = "arrow-active" if is_election_path else "arrow"
            dash_array = 'stroke-dasharray="6,4"' if is_election_path else ''
            svg_lines.append(
                f'  <line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" '
                f'stroke="{stroke_color}" stroke-width="2.5" {dash_array} marker-end="url(#{marker_id})" />'
            )

    for pid in ids:
        st = states[pid]
        px, py = pos[pid]

        if not st["is_active"]:
            fill_color = "#FEE2E2"
            stroke_color = "#EF4444"
            status_text = "FALHADO"
        elif stage_num in [1, 4] and st["is_active"]:
            fill_color = "#DBEAFE"
            stroke_color = "#3B82F6"
            status_text = "ELEICAO"
        elif st["coordinator"] == pid:
            fill_color = "#FEF3C7"
            stroke_color = "#F59E0B"
            status_text = "COORDENADOR"
        elif st["in_election"]:
            fill_color = "#DBEAFE"
            stroke_color = "#3B82F6"
            status_text = "ELEICAO"
        else:
            fill_color = "#E2E8F0"
            stroke_color = "#64748B"
            status_text = f"Coord: {st['coordinator']}" if st['coordinator'] else "ATIVO"

        svg_lines.append('  <g filter="url(#shadow)">')
        svg_lines.append(f'    <circle cx="{px:.1f}" cy="{py:.1f}" r="32" fill="{fill_color}" stroke="{stroke_color}" stroke-width="3" />')
        svg_lines.append(f'    <text x="{px:.1f}" y="{py-5:.1f}" font-family="system-ui, sans-serif" font-size="15" font-weight="bold" text-anchor="middle" fill="#0F172A">P{pid}</text>')
        svg_lines.append(f'    <text x="{px:.1f}" y="{py+14:.1f}" font-family="system-ui, sans-serif" font-size="10" font-weight="600" text-anchor="middle" fill="#475569">{status_text}</text>')
        svg_lines.append('  </g>')

    svg_lines.extend([
        '  <g transform="translate(25, 595)">',
        '    <rect width="600" height="65" fill="#FFFFFF" stroke="#CBD5E1" rx="8" />',
        '    <circle cx="25" cy="32" r="9" fill="#E2E8F0" stroke="#64748B" stroke-width="2"/>',
        '    <text x="40" y="36" font-family="system-ui, sans-serif" font-size="11" fill="#334155">Ativo</text>',
        '    <circle cx="120" cy="32" r="9" fill="#FEF3C7" stroke="#F59E0B" stroke-width="2"/>',
        '    <text x="135" y="36" font-family="system-ui, sans-serif" font-size="11" fill="#334155">Coordenador</text>',
        '    <circle cx="235" cy="32" r="9" fill="#DBEAFE" stroke="#3B82F6" stroke-width="2"/>',
        '    <text x="250" y="36" font-family="system-ui, sans-serif" font-size="11" fill="#334155">Eleicao</text>',
        '    <circle cx="330" cy="32" r="9" fill="#FEE2E2" stroke="#EF4444" stroke-width="2"/>',
        '    <text x="345" y="36" font-family="system-ui, sans-serif" font-size="11" fill="#334155">Falhado</text>',
        '    <line x1="420" y1="32" x2="450" y2="32" stroke="#2563EB" stroke-width="2" stroke-dasharray="4,3"/>',
        '    <text x="458" y="36" font-family="system-ui, sans-serif" font-size="11" fill="#334155">Fluxo TCP</text>',
        '  </g>',
        '</svg>'
    ])

    with open(filepath, "w", encoding="utf-8") as f:
        f.write("\n".join(svg_lines))


def demo():
    log_file_path = os.path.join("representacao", "log_execucao_resultado.txt")
    collector = LogCollectorServer(log_file_path)
    collector.start()
    time.sleep(0.4)

    print("=" * 80)
    print("DEMONSTRACAO DO SISTEMA DISTRIBUIDO: ELEICAO EM ANEL (SOCKETS TCP)")
    print("=" * 80)

    sorted_ids = sorted(NOS)
    coordinator_id = max(sorted_ids)
    initiators = sorted(set([30, 60, coordinator_id]))

    ring = RingController(sorted_ids)
    ring.start_all()

    print("\n" + "-" * 80)
    print("PASSO 1: Eleicao inicial")
    print("-" * 80)
    ring.send_log("INFO", f"--- ETAPA 1: ELEICAO INICIAL ({', '.join(f'P{i}' for i in initiators)}) ---")
    for pid in initiators:
        ring.trigger_election(pid)

    generate_svg_stage(ring, 1, "Eleicao Inicial", "estagio_1_eleicao_inicial.svg")
    time.sleep(3.5)

    print("\n" + "-" * 80)
    print("PASSO 2: Definicao e comunicacao do coordenador eleito")
    print("-" * 80)
    states = ring.query_ring_state()
    ring.send_log("INFO", f"--- ETAPA 2: ESTADO DO ANEL APOS ELEICAO INICIAL (COORDENADOR: P{coordinator_id}) ---")
    for pid in sorted_ids:
        st = states[pid]
        coord_str = f"P{st['coordinator']}" if st["coordinator"] is not None else "N/A"
        ring.send_log(
            "INFO",
            f"P{pid:2d} | Coordenador: {coord_str:3s} | Sucessor: P{st['successor_id']}"
        )

    generate_svg_stage(ring, 2, "Definicao e Comunicacao do Coordenador", "estagio_2_coordenador_definido.svg")

    print("\n" + "-" * 80)
    print(f"PASSO 3: Encerramento do coordenador (Simulacao de Falha do P{coordinator_id})")
    print("-" * 80)
    ring.send_log("INFO", f"--- ETAPA 3: FALHA DO COORDENADOR (ENCERRANDO P{coordinator_id}) ---")
    ring.kill_process(coordinator_id)
    time.sleep(1.0)

    generate_svg_stage(ring, 3, "Encerramento do Coordenador", "estagio_3_queda_coordenador.svg")

    print("\n" + "-" * 80)
    print("PASSO 4: Realizacao de uma nova eleicao e reconfiguracao do anel")
    print("-" * 80)
    ring.send_log("INFO", "--- ETAPA 4: NOVA ELEICAO E RECONFIGURACAO DO ANEL ---")
    surviving_ids = [pid for pid in sorted_ids if pid != coordinator_id]

    for pid in surviving_ids:
        ring.trigger_election(pid)

    time.sleep(0.15)
    generate_svg_stage(ring, 4, "Realizacao de Nova Eleicao", "estagio_4_nova_eleicao.svg")
    time.sleep(4.0)

    print("\n" + "-" * 80)
    print("PASSO 5: Definição do novo coordenador")
    print("-" * 80)
    final_states = ring.query_ring_state()
    new_active_ids = [pid for pid, st in final_states.items() if st["is_active"]]
    new_coordinator_id = max(new_active_ids)

    ring.send_log("INFO", f"--- ETAPA 5: ANEL RECONFIGURADO (NOVO COORDENADOR: P{new_coordinator_id}) ---")
    for pid in sorted_ids:
        st = final_states[pid]
        status = "ATIVO" if st["is_active"] else "INATIVO"
        succ_str = f"P{st['successor_id']}" if st["is_active"] else "N/A"
        coord_str = f"P{st['coordinator']}" if st["is_active"] and st["coordinator"] is not None else "N/A"
        ring.send_log(
            "INFO",
            f"P{pid:2d} [{status:7s}] | Coordenador: {coord_str:3s} | Sucessor: {succ_str:3s}"
        )

    ring.send_log("INFO", f"Resumo final -> Novo coordenador eleito: P{new_coordinator_id}")
    generate_svg_stage(ring, 5, "Definicao do Novo Coordenador", "estagio_5_novo_coordenador.svg")

    print("\n" + "_" * 80)
    print("DEMONSTRAÇÃO CONCLUÍDA COM SUCESSO.")
    print("=" * 80)
    ring.stop_all()
    time.sleep(0.5)

    collector.shutdown()
    collector.join(timeout=2.0)
    print(f"\nLog unico exportado com sucesso em: {log_file_path}")


if __name__ == "__main__":
    demo()