# Funcionamento do algoritmo

## Arquitetura Concorrente (threading e queue.Queue):
- Cada processo roda em sua própria thread de execução contínua, simulando nós independentes de uma rede distribuída. A comunicação entre os processos ocorre de forma assíncrona por meio de filas thread-safe (mailbox), simulando sockets ou canais de rede de passagem de mensagens.

## Variante Chang-Roberts:

- Quando uma mensagem de eleição ELECTION(id) chega a um processo, seu ID é comparado com o ID local;
- Se o ID recebido for maior, a mensagem é repassada ao sucessor;
- Se o ID recebido for menor, a mensagem é descartada;
- Se o ID recebido for igual ao próprio ID, significa que a mensagem deu uma volta completa no anel, elegendo o criador como o novo coordenador. Em seguida, a mensagem COORDINATOR é disparada para informar todos os nós ativos.

## Detecção de Falhas e Reconfiguração Dinâmica do Anel:
- O método find_active_successor() verifica se o vizinho imediato está ativo (is_active);
- Se o vizinho estiver inativo (como no Passo 3, quando o coordenador 80 é encerrado), o processo pula automaticamente o nó falhado e atualiza o seu ponteiro de sucessor para o próximo nó ativo encontrado, mantendo o anel totalmente íntegro para circulação de mensagens.

## Incrementação
- Para um melhor entendimento dos estágios, a cada estágio do algoritmo de Eleição em Anel, ele gera um arquivo svg;
No final da execução, é gerado um log de todo o processo para uma futura análise.