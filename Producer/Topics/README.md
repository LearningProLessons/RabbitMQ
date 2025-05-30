<div dir="rtl" style="text-align: right;">

# RabbitMQ Topic Exchange - خلاصه و مفید

## چی هست؟

Topic Exchange یه نوع Exchange تو RabbitMQ هست که پیام‌ها رو بر اساس **الگوی کلید مسیر (Routing Key pattern)** به
صف‌ها (Queues) می‌فرسته. این الگو می‌تونه شامل علامت‌های ویژه مثل:

* `*` (ستاره): جایگزین یک کلمه در مسیر
* `#` (هشتگ): جایگزین صفر یا بیشتر کلمه در مسیر

---

## چرا استفاده کنیم؟

* وقتی پیام‌هات خیلی متنوع‌اند و می‌خوای دقیق‌تر فیلتر کنی که کدوم پیام بره کدوم صف
* وقتی چند سرویس یا بخش مختلف از سیستم می‌خوان فقط نوع خاصی از پیام‌ها رو دریافت کنن
* برای طراحی سیستم‌های event-driven قوی و مقیاس‌پذیر، مخصوصاً سیستم‌هایی مثل درگاه پرداخت یا پرداخت‌های آنلاین که خیلی
  رویداد مختلف دارن

---

## مثال دنیای واقعی (پرداخت و درگاه)

فرض کن یه سیستم پرداخت داریم که چند تا رویداد مهم تولید می‌کنه:

| Routing Key      | معنی                    |
|------------------|-------------------------|
| payment.created  | تراکنش پرداخت ایجاد شده |
| payment.failed   | پرداخت ناموفق بوده      |
| refund.succeeded | بازپرداخت موفق          |
| refund.failed    | بازپرداخت ناموفق        |

---

## چطوری صف‌ها رو bind کنیم؟

* صف **Logger** می‌خواد همه رویدادها رو ببینه
  `binding key = "#"` (هر چیزی)

* صف **Accounting** فقط می‌خواد پیام‌های پرداخت رو دریافت کنه
  `binding key = "payment.*"`

* صف **ErrorHandler** فقط خطاها رو می‌خواد، یعنی پیام‌هایی که با `.failed` تموم می‌شن
  `binding key = "*.failed"`

---

## نتیجه

* پیام با routing key `payment.created` میره تو صف Logger و Accounting
* پیام با routing key `refund.failed` میره تو صف Logger و ErrorHandler
* پیام با routing key `refund.succeeded` فقط میره تو صف Logger

---

## خلاصه کد Bind (مثال)

```csharp
await channel.ExchangeDeclareAsync("event-topic-exchange", ExchangeType.Topic);

await channel.QueueBindAsync("logger-queue", "event-topic-exchange", "#");
await channel.QueueBindAsync("accounting-queue", "event-topic-exchange", "payment.*");
await channel.QueueBindAsync("errors-queue", "event-topic-exchange", "*.failed");
```

---

## یه نکته کوچیک

Topic Exchange خیلی انعطاف‌پذیره و الگوهای پیچیده‌تر هم می‌تونی بسازی، ولی در بیشتر موارد این سه الگوی ساده کلی کار رو
راه می‌اندازه.

---



</div>