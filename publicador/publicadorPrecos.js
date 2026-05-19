console.log('Publicador preços sendo executado');

const amqp = require('amqplib');
const rabbitUrl = 'amqp://user:password@rabbitmq:5672';

let conexaoGlobal = null;
let canalGlobal = null;

async function conectar() {
    try {
        console.log("Conectando");

        conexaoGlobal = await amqp.connect(rabbitUrl);

        canalGlobal = await conexaoGlobal.createChannel();

        await canalGlobal.assertQueue('fila_precos', { durable: true });

        console.log("Conexão foi");

        conexaoGlobal.on('close', () => {
            console.error("Conexão com  recnectar em em 5s...");
            canalGlobal = null;
            setTimeout(conectar, 5000);
        });

        conexaoGlobal.on('error', (err) => {
            console.error(" Erro na conexão:", err.message);
        });

    } catch (erro) {
        console.error(" Falha ao conectar", erro.message);
        canalGlobal = null;
        setTimeout(conectar, 5000);
    }
}

function enviarMensagemPreco() {

    if (!canalGlobal) {
        console.log("Canal não está pronto ainda");
        return;
    }

    try {
        const dadosPreco = {
            produto: 'Teclado',
            preco: 299.90,
            timestamp: new Date().toISOString()
        };
        const mensagem = JSON.stringify(dadosPreco);

        canalGlobal.sendToQueue('fila_precos', Buffer.from(mensagem), { persistent: true });
        console.log(`Enviado : ${mensagem}`);
    } catch (erro) {
        console.error(" Erro ao enviar mensagem :", erro.message);
    }
}

conectar();

setInterval(enviarMensagemPreco, 5000);