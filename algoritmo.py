import time
import threading
import queue
import logging
import os
import io
import math

# Captura de log em buffer para exportação única ao final do script
log_capture_string = io.StringIO()
ch = logging.StreamHandler(log_capture_string)
ch.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
ch.setFormatter(formatter)
console_handler.setFormatter(formatter)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(ch)
logger.addHandler(console_handler)

class MessageType:
    ELECTION = "ELECTION"
    COORDINATOR = "COORDINATOR"


class Process(threading.Thread):
    """Representa um processo individual executando no anel distribuído."""
    
    def __init__(self, process_id):
        super().__init__()
        self.process_id = process_id
        self.mailbox = queue.Queue()
        self.is_active = True
        self.successor = None
        self.coordinator = None
        self.in_election = False
        self._stop_event = threading.Event()
        self.daemon = True

    def run(self):
        while not self._stop_event.is_set():
            try:
                msg = self.mailbox.get(timeout=0.3)
                if not self.is_active:
                    self.mailbox.task_done()
                    continue
                self.handle_message(msg)
                self.mailbox.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                if self._stop_event.is_set():
                    break
                logging.error(f"Erro no Processo {self.process_id}: {e}")

    def handle_message(self, msg):
        if not self.is_active:
            return
            
        m_type = msg.get("type")
        
        if m_type == MessageType.ELECTION:
            origin_id = msg.get("origin_id")
            logging.info(f"[Processo {self.process_id}] Recebeu ELECTION(id={origin_id})")
            
            if origin_id > self.process_id:
                self.forward_message(msg)
            elif origin_id < self.process_id:
                logging.info(f"[Processo {self.process_id}] Meu ID ({self.process_id}) é maior que {origin_id}. Descartando mensagem.")
            elif origin_id == self.process_id:
                logging.info(f"[Processo {self.process_id}] Meu ID completou a volta no anel! EU SOU O NOVO COORDENADOR.")
                self.in_election = False
                self.announce_coordinator()

        elif m_type == MessageType.COORDINATOR:
            coord_id = msg.get("coordinator_id")
            if self.coordinator != coord_id:
                self.coordinator = coord_id
                self.in_election = False
                logging.info(f"[Processo {self.process_id}] Reconhece Processo {coord_id} como Coordenador oficial.")
                if coord_id != self.process_id:
                    self.forward_message(msg)

    def start_election(self):
        if not self.is_active:
            return
        self.in_election = True
        msg = {
            "type": MessageType.ELECTION,
            "origin_id": self.process_id
        }
        logging.info(f"[Processo {self.process_id}] Inicia eleição enviando ID {self.process_id} para o sucessor.")
        self.forward_message(msg)

    def announce_coordinator(self):
        self.coordinator = self.process_id
        msg = {
            "type": MessageType.COORDINATOR,
            "coordinator_id": self.process_id
        }
        logging.info(f"[Processo {self.process_id}] Anuncia vitória do Coordenador {self.process_id} para o anel.")
        self.forward_message(msg)

    def forward_message(self, msg):
        target = self.find_active_successor()
        if target:
            time.sleep(0.05)
            target.mailbox.put(msg)
            logging.info(f"[Processo {self.process_id}] Encaminhou msg para Sucessor [Processo {target.process_id}]")
        else:
            logging.error(f"[Processo {self.process_id}] Nenhum sucessor ativo encontrado no anel!")

    def find_active_successor(self):
        curr = self.successor
        steps = 0
        while curr and not curr.is_active and steps < 10:
            logging.warning(f"[Processo {self.process_id}] Sucessor [Processo {curr.process_id}] está inativo/morto. Pulando...")
            curr = curr.successor
            steps += 1
            
        if curr and curr.is_active and curr != self.successor:
            logging.info(f"[Reconfiguração] Processo {self.process_id} atualizou seu sucessor direto para o Processo {curr.process_id}.")
            self.successor = curr
            
        return curr if (curr and curr.is_active) else None

    def stop(self):
        self.is_active = False
        self._stop_event.set()
        try:
            self.mailbox.put({"type": "DUMMY"})
        except:
            pass


class RingSystem:
    def __init__(self, ids=[10, 20, 30, 40, 50, 60, 70, 80]):
        self.processes = {}
        for pid in ids:
            self.processes[pid] = Process(pid)
            
        self.ids = sorted(ids)
        n = len(self.ids)
        for i in range(n):
            current_id = self.ids[i]
            successor_id = self.ids[(i + 1) % n]
            self.processes[current_id].successor = self.processes[successor_id]

    def start_all(self):
        for p in self.processes.values():
            p.start()
        logging.info(f"Anel distribuído iniciado com sucesso com {len(self.processes)} processos: {self.ids}")

    def stop_all(self):
        for p in self.processes.values():
            p.stop()
        for p in self.processes.values():
            p.join(timeout=1.0)
        logging.info("Todos os processos da simulação foram encerrados.")

    def get_process(self, pid):
        return self.processes.get(pid)


def generate_svg_stage(ring_system, stage_num, stage_title, filename):
    """Gera arquivos SVG representando o estado do anel a cada estágio na pasta 'representacao'."""
    os.makedirs("representacao", exist_ok=True)
    filepath = os.path.join("representacao", filename)
    
    width, height = 650, 650
    cx, cy, radius = 325, 325, 200
    ids = ring_system.ids
    n = len(ids)
    
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
        f'  <text x="{cx}" y="45" font-family="system-ui, sans-serif" font-size="20" font-weight="bold" text-anchor="middle" fill="#1E293B">Estágio {stage_num}: {stage_title}</text>',
    ]
    
    pos = {}
    for i, pid in enumerate(ids):
        angle = 2 * math.pi * i / n - math.pi / 2
        px = cx + radius * math.cos(angle)
        py = cy + radius * math.sin(angle)
        pos[pid] = (px, py)

    # Renderização das conexões entre processos
    for pid in ids:
        p = ring_system.get_process(pid)
        if not p.is_active:
            continue
        succ = p.find_active_successor()
        if succ:
            x1, y1 = pos[pid]
            x2, y2 = pos[succ.process_id]
            is_election_path = (stage_num in [1, 4])
            stroke_color = "#2563EB" if is_election_path else "#94A3B8"
            marker_id = "arrow-active" if is_election_path else "arrow"
            dash_array = 'stroke-dasharray="6,4"' if is_election_path else ''
            svg_lines.append(f'  <line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" stroke="{stroke_color}" stroke-width="2.5" {dash_array} marker-end="url(#{marker_id})" />')

    # Renderização dos nós
    for pid in ids:
        p = ring_system.get_process(pid)
        px, py = pos[pid]
        
        if not p.is_active:
            fill_color = "#FEE2E2"
            stroke_color = "#EF4444"
            status_text = "FALHADO"
        elif p.coordinator == pid:
            fill_color = "#FEF3C7"
            stroke_color = "#F59E0B"
            status_text = "COORDENADOR"
        elif p.in_election or (stage_num in [1, 4] and p.is_active):
            fill_color = "#DBEAFE"
            stroke_color = "#3B82F6"
            status_text = "ELEIÇÃO"
        else:
            fill_color = "#E2E8F0"
            stroke_color = "#64748B"
            status_text = f"Coord: {p.coordinator}"

        svg_lines.append('  <g filter="url(#shadow)">')
        svg_lines.append(f'    <circle cx="{px:.1f}" cy="{py:.1f}" r="32" fill="{fill_color}" stroke="{stroke_color}" stroke-width="3" />')
        svg_lines.append(f'    <text x="{px:.1f}" y="{py-5:.1f}" font-family="system-ui, sans-serif" font-size="15" font-weight="bold" text-anchor="middle" fill="#0F172A">P{pid}</text>')
        svg_lines.append(f'    <text x="{px:.1f}" y="{py+14:.1f}" font-family="system-ui, sans-serif" font-size="10" font-weight="600" text-anchor="middle" fill="#475569">{status_text}</text>')
        svg_lines.append('  </g>')

    svg_lines.extend([
        '  <g transform="translate(45, 570)">',
        '    <rect width="560" height="60" fill="#FFFFFF" stroke="#E2E8F0" rx="8" />',
        '    <circle cx="20" cy="30" r="10" fill="#DBEAFE" stroke="#3B82F6" stroke-width="2"/>',
        '    <text x="36" y="34" font-family="system-ui, sans-serif" font-size="11" fill="#334155">Em Eleição</text>',
        '    <circle cx="150" cy="30" r="10" fill="#FEF3C7" stroke="#F59E0B" stroke-width="2"/>',
        '    <text x="166" y="34" font-family="system-ui, sans-serif" font-size="11" fill="#334155">Coordenador</text>',
        '    <circle cx="290" cy="30" r="10" fill="#FEE2E2" stroke="#EF4444" stroke-width="2"/>',
        '    <text x="306" y="34" font-family="system-ui, sans-serif" font-size="11" fill="#334155">Falhado</text>',
        '    <line x1="410" y1="30" x2="440" y2="30" stroke="#2563EB" stroke-width="2" stroke-dasharray="4,3"/>',
        '    <text x="448" y="34" font-family="system-ui, sans-serif" font-size="11" fill="#334155">Fluxo de Eleição</text>',
        '  </g>',
        '</svg>'
    ])
    
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("\n".join(svg_lines))


def demo():
    print("="*80)
    print("DEMONSTRAÇÃO DO SISTEMA DISTRIBUÍDO: ELEIÇÃO EM ANEL (CHANG-ROBERTS)")
    print("="*80)

    ring_ids = [10, 20, 30, 40, 50, 60, 70, 80]
    ring = RingSystem(ring_ids)
    ring.start_all()
    time.sleep(0.5)

    # ESTÁGIO 1: Eleição inicial
    print("\n" + "-"*80)
    print("PASSO 1: Eleição inicial")
    print("-"*80)
    logging.info("Disparando eleição inicial no anel a partir dos processos 30, 60 e 80...")
    for pid in [30, 60, 80]:
        ring.get_process(pid).start_election()
    
    generate_svg_stage(ring, 1, "Eleição Inicial", "estagio_1_eleicao_inicial.svg")
    time.sleep(3.0)

    # ESTÁGIO 2: Definição e comunicação do coordenador
    print("\n" + "-"*80)
    print("PASSO 2: Definição e comunicação do coordenador eleito")
    print("-"*80)
    coordinator_id = max(ring_ids)
    logging.info(f"Estado atual do anel após a eleição inicial:")
    for pid in ring_ids:
        p = ring.get_process(pid)
        logging.info(f"Processo {pid:2d} | Coordenador Conhecido: {p.coordinator} | Sucessor Ativo: {p.find_active_successor().process_id}")

    generate_svg_stage(ring, 2, "Definição e Comunicação do Coordenador", "estagio_2_coordenador_definido.svg")

    # ESTÁGIO 3: Encerramento do coordenador
    print("\n" + "-"*80)
    print("PASSO 3: Encerramento do coordenador (Simulação de Falha)")
    print("-"*80)
    current_coord_proc = ring.get_process(coordinator_id)
    if current_coord_proc:
        logging.info(f"Simulando falha repentina do Coordenador atual [Processo {coordinator_id}]...")
        current_coord_proc.stop()
        time.sleep(1.0)

    generate_svg_stage(ring, 3, "Encerramento do Coordenador", "estagio_3_queda_coordenador.svg")

    # ESTÁGIO 4: Realização de nova eleição
    print("\n" + "-"*80)
    print("PASSO 4: Realização de uma nova eleição e reconfiguração do anel")
    print("-"*80)
    logging.info("Detectando queda do coordenador e disparando nova eleição com processos restantes...")
    surviving_ids = [pid for pid in ring_ids if pid != coordinator_id]
    
    for pid in surviving_ids:
        ring.get_process(pid).start_election()

    generate_svg_stage(ring, 4, "Realização de Nova Eleição", "estagio_4_nova_eleicao.svg")
    time.sleep(3.0)

    # ESTÁGIO 5: Definição do novo coordenador
    print("\n" + "-"*80)
    print("PASSO 5: Definição do novo coordenador")
    print("-"*80)
    new_active_ids = [pid for pid in ring_ids if ring.get_process(pid).is_active]
    new_coordinator_id = max(new_active_ids)
    
    logging.info(f"Verificação final do anel reconfigurado:")
    for pid in ring_ids:
        p = ring.get_process(pid)
        status = "ATIVO" if p.is_active else "MORTO/INATIVO"
        succ = p.find_active_successor() if p.is_active else None
        logging.info(f"Processo {pid:2d} [{status:13s}] | Coordenador: {p.coordinator} | Sucessor Ativo: {succ.process_id if succ else 'N/A'}")

    logging.info(f"Novo coordenador eleito com sucesso: Processo {new_coordinator_id}")
    generate_svg_stage(ring, 5, "Definição do Novo Coordenador", "estagio_5_novo_coordenador.svg")

    print("\n" + "="*80)
    print("DEMONSTRAÇÃO CONCLUÍDA COM SUCESSO.")
    print("="*80)
    ring.stop_all()

    # Exporta todo o log da execução sem segmentar por estágios para um único TXT
    log_file_path = os.path.join("representacao", "log_execucao_resultado.txt")
    with open(log_file_path, "w", encoding="utf-8") as f:
        f.write(log_capture_string.getvalue())
    print(f"\nLog único exportado com sucesso em: {log_file_path}")

if __name__ == "__main__":
    demo()