console.log('Publicador preços sendo executado')

const amqp = require('amqplib');
async function publicar() {
    const rabbitUrl = 'amqp://user:password@rabbitmq:5672';
    try{
        const conexao = await amqp.connect(rabbitUrl)
        const canal = await conexao.createChannel()
        const nome_da_fila = 'precos_produtos'
        const nomeFila = 'precos_produtos';
        await canal.assertQueue(nomeFila, { durable: true });
        const dadosPreco = {
            produto: 'teclado',
            preco: 299.90,
        };
        const mensagem = JSON.stringify(dadosPreco);
        canal.sendToQueue(nomeFila, Buffer.from(mensagem), { persistent: true });
        console.log("Conexão fechada com sucesso.");
    }catch{
console.error("Erro ao publicar mensagem:", erro);
    }
}
setInterval(() => {
    publicar();
}, 3000);

setInterval(() => {}, 1000 * 60 * 60);