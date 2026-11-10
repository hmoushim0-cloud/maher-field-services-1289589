package sa.factory.maherfieldservices.data.repository

import sa.factory.maherfieldservices.domain.repository.MaherFieldRepository
import sa.factory.maherfieldservices.hardware.HardwareSensorDriver
import sa.factory.maherfieldservices.model.MaherFieldItem
import sa.factory.maherfieldservices.model.MaherFieldOrder
import sa.factory.maherfieldservices.model.MaherFieldTelemetry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch

class MaherFieldRepositoryImpl(
    private val sensorDriver: HardwareSensorDriver
) : MaherFieldRepository {

    private val _catalog = MutableStateFlow<List<MaherFieldItem>>(listOf(
        MaherFieldItem("1", "تحسين أحداث التطبيق", "ضبط دقيق لأحداث التحويل الداخلية باستخدام خوارزميات تعلم آلي", 150.0, "معتمد", "إدارة حملات"),
        MaherFieldItem("2", "إدارة حملات AppsFlyer", "تكامل كامل مع منصة AppsFlyer لتحليل مسارات التحويل المتعددة", 280.0, "فوري", "تحليل أحداث"),
        MaherFieldItem("3", "تحليل التحويلات الجغرافي", "ربط بيانات GPS مع أحداث التطبيق لتحديد أنماط المستخدمين", 95.0, "جديد", "استهداف متقدم")
    ))
    override val catalogFlow: Flow<List<MaherFieldItem>> = _catalog.asStateFlow()

    private val _cart = MutableStateFlow<List<MaherFieldItem>>(emptyList())
    override val activeCart: StateFlow<List<MaherFieldItem>> = _cart.asStateFlow()

    override val telemetryFlow: Flow<MaherFieldTelemetry> = sensorDriver.observePeripheralStream()
        .map { frame ->
            MaherFieldTelemetry(
                peripheralName = frame.peripheralName,
                status = frame.connectionState,
                liveReading = frame.livePayload,
                batteryPct = 96,
                signalDbm = -54
            )
        }

    override fun addToCart(item: MaherFieldItem) {
        val current = _cart.value.toMutableList()
        current.add(item)
        _cart.value = current
    }

    override fun removeFromCart(item: MaherFieldItem) {
        val current = _cart.value.toMutableList()
        current.remove(item)
        _cart.value = current
    }

    override fun clearCart() {
        _cart.value = emptyList()
    }

    override suspend fun checkout(paymentMethod: String): MaherFieldOrder {
        val currentItems = _cart.value.toList()
        val subtotal = currentItems.sumOf { it.priceSar }
        val vat = subtotal * 0.15
        val total = subtotal + vat
        val order = MaherFieldOrder(
            orderId = "ORD-${System.currentTimeMillis() % 100000}",
            items = currentItems,
            subtotalSar = subtotal,
            vatSar = vat,
            totalSar = total,
            paymentMethod = paymentMethod
        )
        clearCart()
        return order
    }

    override suspend fun refreshCatalog() {
        // Offline-first sync logic
    }
}
