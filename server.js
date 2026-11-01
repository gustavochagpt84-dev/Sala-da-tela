// Sala da Tela - servidor de sinalização (v2)
// Ele só apresenta as pessoas umas às outras. O vídeo vai direto entre os navegadores (WebRTC).
//
// Novidades da v2:
// - Cada pessoa tem um id fixo (gerado pelo navegador). Se a conexão cair e voltar,
//   ela continua sendo a mesma pessoa, sem "sair" e "entrar" de novo.
// - Quando a conexão cai, o servidor espera um tempo (TOLERANCIA_MS) antes de avisar
//   que a pessoa saiu. Assim, trocar de app no celular não derruba ninguém.
// - O servidor sabe quem está transmitindo, para avisar quem chega ou reconecta.
// - Ping periódico para remover conexões "fantasmas".

const http = require("http");
const fs = require("fs");
const path = require("path");
const { WebSocketServer } = require("ws");

const PORTA = process.env.PORT || 3000;
const TOLERANCIA_MS = 45000; // tempo que a pessoa pode ficar desconectada sem "sair"

const servidor = http.createServer((req, res) => {
  if (req.url === "/" || req.url.startsWith("/?")) {
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-cache" });
    fs.createReadStream(path.join(__dirname, "public", "index.html")).pipe(res);
  } else if (req.url === "/saude") {
    res.writeHead(200);
    res.end("ok");
  } else {
    res.writeHead(404);
    res.end("Não encontrado");
  }
});

const wss = new WebSocketServer({ server: servidor });

// salas = { nome: { pessoas: Map(id -> { ws, nome, timerSaida }), transmissor: id|null } }
const salas = {};

function enviar(ws, msg) {
  if (ws && ws.readyState === ws.OPEN) ws.send(JSON.stringify(msg));
}

function paraTodos(sala, msg, excetoId) {
  for (const [pid, p] of sala.pessoas) {
    if (pid !== excetoId) enviar(p.ws, msg);
  }
}

function removerPessoa(nomeSala, id) {
  const sala = salas[nomeSala];
  if (!sala) return;
  sala.pessoas.delete(id);
  if (sala.transmissor === id) sala.transmissor = null;
  paraTodos(sala, { type: "saiu", id });
  if (sala.pessoas.size === 0) delete salas[nomeSala];
}

wss.on("connection", (ws) => {
  let id = null;
  let nomeSala = null;
  ws.vivo = true;
  ws.on("pong", () => (ws.vivo = true));

  ws.on("message", (bruto) => {
    let msg;
    try {
      msg = JSON.parse(bruto);
    } catch {
      return;
    }

    if (msg.type === "ping") {
      enviar(ws, { type: "pong" });
      return;
    }

    if (msg.type === "entrar") {
      id = String(msg.id || Math.random()).slice(0, 40);
      nomeSala = String(msg.sala || "geral").slice(0, 40);
      const nome = String(msg.nome || "Anônimo").slice(0, 30);

      if (!salas[nomeSala]) salas[nomeSala] = { pessoas: new Map(), transmissor: null };
      const sala = salas[nomeSala];
      const existente = sala.pessoas.get(id);

      if (existente) {
        // A pessoa voltou depois de uma queda: troca só a conexão
        clearTimeout(existente.timerSaida);
        if (existente.ws !== ws) existente.ws.close();
        existente.ws = ws;
        existente.nome = nome;
      } else {
        sala.pessoas.set(id, { ws, nome, timerSaida: null });
        paraTodos(sala, { type: "entrou", id, nome }, id);
      }

      // Se quem reconectou é o transmissor (ex.: servidor reiniciou), ele avisa de novo
      if (msg.compartilhando) sala.transmissor = id;

      const pessoas = [...sala.pessoas.entries()]
        .filter(([pid]) => pid !== id)
        .map(([pid, p]) => ({ id: pid, nome: p.nome }));
      enviar(ws, { type: "bem-vindo", pessoas, transmissor: sala.transmissor });
      return;
    }

    if (!nomeSala || !salas[nomeSala]) return;
    const sala = salas[nomeSala];

    // Mensagem para uma pessoa específica (dados do WebRTC)
    if (msg.type === "sinal") {
      const alvo = sala.pessoas.get(String(msg.para));
      if (alvo) enviar(alvo.ws, { type: "sinal", de: id, dados: msg.dados });
      return;
    }

    // Aviso para todo mundo da sala
    if (msg.type === "aviso") {
      if (msg.dados && msg.dados.compartilhando) sala.transmissor = id;
      if (msg.dados && msg.dados.parou && sala.transmissor === id) sala.transmissor = null;
      paraTodos(sala, { type: "aviso", de: id, dados: msg.dados }, id);
      return;
    }

    if (msg.type === "sair") {
      removerPessoa(nomeSala, id);
      nomeSala = null;
    }
  });

  ws.on("close", () => {
    if (!nomeSala || !salas[nomeSala]) return;
    const pessoa = salas[nomeSala].pessoas.get(id);
    // Só conta como saída se essa ainda for a conexão atual da pessoa
    if (!pessoa || pessoa.ws !== ws) return;
    const salaDaPessoa = nomeSala;
    pessoa.timerSaida = setTimeout(() => removerPessoa(salaDaPessoa, id), TOLERANCIA_MS);
  });
});

// A cada 30 s, derruba conexões que pararam de responder
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.vivo) {
      ws.terminate();
      continue;
    }
    ws.vivo = false;
    ws.ping();
  }
}, 30000);

servidor.listen(PORTA, () => {
  console.log(`Sala da Tela rodando em http://localhost:${PORTA}`);
});
