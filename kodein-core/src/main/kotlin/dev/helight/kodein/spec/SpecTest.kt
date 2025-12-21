package dev.helight.kodein.spec

import dev.helight.kodein.Kodein
import dev.helight.kodein.collection.KPageCursor
import dev.helight.kodein.dsl.buildFilter
import dev.helight.kodein.dsl.buildUpdate
import dev.helight.kodein.dsl.crudScope
import dev.helight.kodein.dsl.findOptions
import dev.helight.kodein.dsl.scope
import dev.helight.kodein.memory.MemoryDocumentDatabase
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import org.bson.BsonValue
import org.bson.codecs.pojo.annotations.BsonId

@Serializable
data class Example(
    var name: String,
    var address: Address?,
) : BaseDocument() {
    companion object Spec : TypedCollectionSpec<Example>(Example::class) {
        val name = field(Example::name).indexed()
        val address = embeddedField(Example::address, Address)
    }
}

@Serializable
data class Address(
    var street: String,
    var city: String,
    var country: Country
) {

    companion object Spec : TypedCollectionSpec<Address>(Address::class) {
        val street = field(Address::street)
        val city = field(Address::city).indexed()
        val country = embeddedField(Address::country, Country)
    }
}

@Serializable
data class Country(
    var name: String,
    var code: String,
    var language: Lang
) {

    companion object Spec : TypedCollectionSpec<Country>(Country::class) {
        val name = field(Country::name)
        val code = field(Country::code).unique()
        val language = embeddedField(Country::language, Lang)
    }
}

@Serializable
data class Lang(
    var code: String,
)  {
    companion object Spec : TypedCollectionSpec<Lang>(Lang::class) {
        val code = field(Lang::code).unique()
    }
}


object UntypedSpec : CollectionSpec() {
    val name = field<String>("name")
    val address = embeddedField("address", Address)
}

fun test(): Unit = runBlocking {
    val col = MemoryDocumentDatabase(Kodein()).getCollection("kodein")


    val a = Example.Spec::address
    val b = Example.Spec::name

    val selector0 = Example.address / Address.country / Country.language
    val selector1 = Example.address.cd { country }.select { language }
    val selector2 = "address.country.language"
    val selector3 = Example.address / "country" / "language"

    findOptions {
        fields(Example.address)
        sortAsc(Example.address).sortDesc(Example.name)
    }

    buildFilter {
        Example.name eq "John Doe"
        Example.name / Country.language / Lang.code eq "en"
        Example.address.cd { country }.cd { language }.select { code } eq "de"
    }

    buildUpdate {
        Example.name set "Jane Doe"
        Example.address.cd { country }.select { name } set "Germany"
        Example.address set Address(
            street = "New Street",
            city = "New City",
            country = Country(
                name = "New Country",
                code = "NC",
                language = Lang("nc")
            )
        )
    }

    Example.scope(col) { collection ->
        val a = collection.find {
            byId("")
            name eq "John Doe"
            address.name eq "Main St"
        }.toList()
    }

    Example.crudScope(col) { collection ->
        val saved = collection.save(
            Example(
                name = "John Doe",
                address = null
            )
        )

        collection.update(saved) {
            name set "Jane Doe"
            address.cd { country }.cd { language }.select { code } set "de"
        }

        collection.save(saved) { copy(name = name + "test") }
    }
}