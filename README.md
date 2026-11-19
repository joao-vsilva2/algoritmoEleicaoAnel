# Simulação de Eleição em Anel

Este projeto simula um sistema distribuído utilizando o **algoritmo de eleição em anel** em Python, com processos independentes comunicando-se exclusivamente via **Sockets TCP**. Durante a execução, o sistema registra os eventos em um arquivo de log centralizado e gera representações visuais em **SVG** para cada estágio da simulação.

---

## Visão Geral da Arquitetura

* **`RingNodeProcess`**: representa cada nó do anel como um processo independente do sistema operacional, escutando conexões TCP em `BASE_PORT + process_id`, encaminhando mensagens `ELECTION` e `COORDINATOR`, atualizando seu sucessor em caso de falha/timeout e reportando eventos ao coletor de logs.
* **`LogCollectorServer`**: servidor TCP auxiliar que centraliza as mensagens de `INFO`, `WARNING` e `ERROR` de todos os nós, exibindo no console e gravando o resultado formatado em `representacao/log_execucao_resultado.txt`.
* **`RingController`**: gerencia o ciclo de vida dos processos do anel (inicialização, disparo de eleições, encerramento de nós para simular falhas, consulta de status e encerramento final).
* **`generate_svg_stage`**: consulta o estado atual do anel e gera diagramas SVG na pasta `representacao/` ilustrando cada etapa da simulação.

---

## Etapas da Simulação (`demo`)

1. **Eleição Inicial (`estagio_1_eleicao_inicial.svg`)**: dispara eleições no anel e captura o estado de eleição dos nós ativos.
2. **Definição e Comunicação do Coordenador (`estagio_2_coordenador_definido.svg`)**: registra o coordenador eleito (nó de maior ID ativo) e o sucessor TCP de cada processo.
3. **Encerramento do Coordenador (`estagio_3_queda_coordenador.svg`)**: simula a falha encerrando o processo do coordenador atual.
4. **Nova Eleição e Reconfiguração (`estagio_4_nova_eleicao.svg`)**: dispara nova eleição entre os processos sobreviventes, que pulam vizinhos inativos e reconfiguram o anel.
5. **Definição do Novo Coordenador (`estagio_5_novo_coordenador.svg`)**: consolida o novo coordenador entre os nós ativos restantes e exibe o estado final do anel.

---

## Como Executar

### Pré-requisitos

* **Python 3.8+** (bibliotecas: `socket`, `json`, `time`, `logging`, `os`, `io`, `math`, `multiprocessing`).

### Executando a simulação

Salve o script (por exemplo, como `algoritmo.py`) e execute:

```bash
python algoritmo.py
```