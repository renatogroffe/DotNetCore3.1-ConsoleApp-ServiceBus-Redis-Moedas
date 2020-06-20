using System;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Configuration;
using StackExchange.Redis;
using Serilog;
using Serilog.Core;
using ProcessadorCotacoes.Models;
using ProcessadorCotacoes.Validators;

namespace ProcessadorCotacoes
{
    class Program
    {
        private static IConfiguration _configuration;
        private static ISubscriptionClient _subscriptionClient;
        private static Logger _logger;
        private static ConnectionMultiplexer _conexaoRedis;

        async static Task Main(string[] args)
        {
            _logger = new LoggerConfiguration()
                .WriteTo.Console()
                .CreateLogger();
            _logger.Information("Testando o consumo de mensagens com Azure Service Bus");

            _logger.Information("Carregando configurações...");
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile($"appsettings.json");
            _configuration = builder.Build();

            string nomeTopic = _configuration["AzureServiceBus:Topic"];
            string subscription = _configuration["AzureServiceBus:Subscription"];
            _subscriptionClient = new SubscriptionClient(
                _configuration["AzureServiceBus:ConnectionString"],
                nomeTopic, subscription);

            _logger.Information($"Topic = {nomeTopic}");
            _logger.Information($"Subscription = {nomeTopic}");

            _conexaoRedis =
                ConnectionMultiplexer.Connect(_configuration["BaseCotacoes"]);

            _logger.Information("Aguardando mensagens...");
            _logger.Information("Pressione Enter para encerrar");
            RegisterOnMessageHandlerAndReceiveMessages();

            Console.ReadLine();
            await _subscriptionClient.CloseAsync();
            _logger.Warning("Encerrando o processamento de mensagens!");
        }

        static void RegisterOnMessageHandlerAndReceiveMessages()
        {
            var messageHandlerOptions = new MessageHandlerOptions(
                ExceptionReceivedHandler)
            {
                MaxConcurrentCalls = 1,
                AutoComplete = false
            };

            _subscriptionClient.RegisterMessageHandler(
                ProcessMessagesAsync, messageHandlerOptions);
        }

        static async Task ProcessMessagesAsync(Message message, CancellationToken token)
        {
            string dados = Encoding.UTF8.GetString(message.Body);
            _logger.Information($"Mensagem recebida: {dados}");

            var cotacao = JsonSerializer.Deserialize<CotacaoMoeda>(dados,
                new JsonSerializerOptions()
                {
                    PropertyNameCaseInsensitive = true
                });

            var validationResult = new CotacaoMoedaValidator().Validate(cotacao);
            if (validationResult.IsValid)
            {
                cotacao.UltimaAtualizacao = DateTime.Now;
                var dbRedis = _conexaoRedis.GetDatabase();
                dbRedis.StringSet(
                    "COTACAO-" + cotacao.Sigla,
                    JsonSerializer.Serialize(cotacao),
                    expiry: null);

                _logger.Information("Cotação registrada com sucesso!");

                using (var clientLogicAppSlack = new HttpClient())
                {
                    clientLogicAppSlack.BaseAddress = new Uri(
                        _configuration["UrlLogicAppAlerta"]);
                    clientLogicAppSlack.DefaultRequestHeaders.Accept.Clear();
                    clientLogicAppSlack.DefaultRequestHeaders.Accept.Add(
                        new MediaTypeWithQualityHeaderValue("application/json"));

                    var requestMessage =
                          new HttpRequestMessage(HttpMethod.Post, String.Empty);

                    requestMessage.Content = new StringContent(
                        JsonSerializer.Serialize(new
                        {
                            moeda = cotacao.Sigla,
                            valor = cotacao.Valor,
                        }), Encoding.UTF8, "application/json");

                    var respLogicApp = clientLogicAppSlack
                        .SendAsync(requestMessage).Result;
                    respLogicApp.EnsureSuccessStatusCode();

                    _logger.Information(
                        "Envio de alerta para Logic App de integração com o Slack");
                }

            }
            else
            {
                _logger.Error("Dados inválidos para a Cotação");
            }

            await _subscriptionClient.CompleteAsync(
                message.SystemProperties.LockToken);
        }

        static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            _logger.Error($"Message handler - Tratamento - Exception: {exceptionReceivedEventArgs.Exception}.");

            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            _logger.Error("Exception context - informaçoes para resolução de problemas:");
            _logger.Error($"- Endpoint: {context.Endpoint}");
            _logger.Error($"- Entity Path: {context.EntityPath}");
            _logger.Error($"- Executing Action: {context.Action}");

            return Task.CompletedTask;
        }
    }
}