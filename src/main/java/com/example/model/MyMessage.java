package com.example.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data // Аннотация Lombok, автоматически генерирует геттеры, сеттеры, toString(), equals() и hashCode()
@AllArgsConstructor // Генерирует конструктор с параметрами для всех полей класса
@NoArgsConstructor // Генерирует конструктор без параметров
public class MyMessage {
    private String message; // Поле для хранения текстового сообщения
    private int id; // Поле для хранения идентификатора сообщения
}