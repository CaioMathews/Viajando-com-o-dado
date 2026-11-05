# Interface

HTML, CSS e módulos JavaScript sem etapa de build. O Nginx é usado no Compose; no desenvolvimento local, `python -m http.server 5173 --bind 127.0.0.1 --directory frontend`.

`app.js` coordena navegação, conexão e atualização; `pipeline.js` desenha o grafo; `details.js` apresenta registros e percursos; `map.js` apresenta o mapa e seus indicadores. O mapa não possui barra de pesquisa.

A pesquisa global consulta `/pipeline/search`, incluindo registros que não chegam à Gold. Um resultado único abre `/pipeline/journey`; resultados múltiplos oferecem uma escolha. O destaque azul usa os nós e as conexões reais do percurso completo, sem depender de cliques para descobrir cada etapa. Métodos são descritos, sem mostrar código-fonte.

No Compose, `config.docker.js` configura a origem `/api`. Localmente, `config.js` aponta para a porta 8000. A opção API permite ajustar ou restaurar o endereço no navegador.

Veja os [READMEs da raiz](../README.md) para fontes, exemplos e execução completa. Os testes locais da interface são ignorados pelo Git conforme a organização solicitada.
