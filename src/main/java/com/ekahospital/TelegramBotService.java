package com.ekahospital;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.api.objects.Update;
import org.telegram.telegrambots.meta.exceptions.TelegramApiException;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class TelegramBotService extends TelegramLongPollingBot {

    @ConfigProperty(name = "alert.telegram.bottoken", defaultValue="")
    String botToken;

    @ConfigProperty(name = "alert.telegram.chatid", defaultValue="")
    String chatId;

    @ConfigProperty(name = "alert.telegram.username", defaultValue="EkaBot")
    String username;

    @Override
    public String getBotUsername() {
        return username;
    }

    @Override
    public String getBotToken() {
        return botToken;
    }

    @Override
    public void onUpdateReceived(Update update) {
        // Not used in this example
    }

    public void sendAlert(String message) {
        SendMessage sendMessage = new SendMessage();
        sendMessage.setChatId(chatId);
        sendMessage.setText(message);

        try {
            execute(sendMessage);
        } catch (TelegramApiException e) {
            e.printStackTrace();
        }
    }
}

