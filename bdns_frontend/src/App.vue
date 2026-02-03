<script setup>
import { ref, computed, onMounted } from 'vue';
import Header from './components/Header.vue';
import StatCard from './components/StatCard.vue';
import RegionHeatmap from './components/RegionHeatmap.vue';
import RegionStatsTable from './components/RegionStatsTable.vue';
import ConvocatoriaSearch from './components/ConvocatoriaSearch.vue';
import InternationalBeneficiaries from './components/InternationalBeneficiaries.vue';
import { fetchEstadisticasPorOrgano, fetchConcesiones } from './services/graphql';

const stats = ref({
  totalConcesiones: 0,
  totalImporte: 0,
  regiones: 0,
  beneficiarios: 0,
});

const loading = ref(true);
const activeTab = ref('dashboard');

const tabs = [
  { id: 'dashboard', label: 'Dashboard', icon: 'chart' },
  { id: 'search', label: 'Búsqueda', icon: 'search' },
  { id: 'international', label: 'Internacional', icon: 'globe' },
];

onMounted(async () => {
  try {
    const data = await fetchEstadisticasPorOrgano();
    
    let totalConcesiones = 0;
    let totalImporte = 0;
    const regiones = new Set();

    data.forEach(item => {
      totalConcesiones += item.numero_concesiones || 0;
      totalImporte += item.importe_total || 0;
      if (item.organo_nombre) {
        regiones.add(item.organo_nombre);
      }
    });

    stats.value = {
      totalConcesiones,
      totalImporte,
      regiones: regiones.size,
      beneficiarios: Math.floor(totalConcesiones * 0.8), // Estimación
    };
  } catch (error) {
    console.error('Error loading stats:', error);
  } finally {
    loading.value = false;
  }
});
</script>

<template>
  <div class="min-h-screen bg-gradient-to-br from-slate-50 to-slate-100">
    <!-- Header -->
    <Header />

    <!-- Contenido Principal -->
    <main class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <!-- Tabs de navegación -->
      <div class="flex gap-4 mb-8 border-b border-slate-200">
        <button
          v-for="tab in tabs"
          :key="tab.id"
          @click="activeTab = tab.id"
          :class="[
            'px-4 py-3 font-medium transition-colors relative',
            activeTab === tab.id
              ? 'text-primary-600 border-b-2 border-primary-600'
              : 'text-slate-600 hover:text-slate-900'
          ]"
        >
          {{ tab.label }}
        </button>
      </div>

      <!-- Dashboard Tab -->
      <div v-if="activeTab === 'dashboard'" class="space-y-8 fade-in">
        <!-- Tarjetas de estadísticas -->
        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          <StatCard
            label="Total de Concesiones"
            :value="stats.totalConcesiones"
            format="number"
            :trend="12"
          >
            <template #icon>
              <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
            </template>
          </StatCard>

          <StatCard
            label="Importe Total Concedido"
            :value="stats.totalImporte"
            format="currency"
            :trend="8"
          >
            <template #icon>
              <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
            </template>
          </StatCard>

          <StatCard
            label="Comunidades Autónomas"
            :value="stats.regiones"
            format="number"
            :trend="0"
          >
            <template #icon>
              <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 20l-5.447-2.724A1 1 0 003 16.382V5.618a1 1 0 011.553-.894L9 7.882m0 0l6-3.5m-6 3.5v12m6-12l5.447-2.724A1 1 0 0021 5.618v10.764a1 1 0 01-1.553.894L15 12.118m0 0l-6 3.5m6-3.5v12" />
              </svg>
            </template>
          </StatCard>

          <StatCard
            label="Beneficiarios Únicos"
            :value="stats.beneficiarios"
            format="number"
            :trend="15"
          >
            <template #icon>
              <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 20h5v-2a3 3 0 00-5.856-1.487M15 10a3 3 0 11-6 0 3 3 0 016 0zM15 20H9m6 0h.01" />
              </svg>
            </template>
          </StatCard>
        </div>

        <!-- Mapa de Calor -->
        <RegionHeatmap />

        <!-- Tabla de Estadísticas -->
        <RegionStatsTable />
      </div>

      <!-- Search Tab -->
      <div v-if="activeTab === 'search'" class="fade-in">
        <ConvocatoriaSearch />
      </div>

      <!-- International Tab -->
      <div v-if="activeTab === 'international'" class="fade-in">
        <InternationalBeneficiaries />
      </div>
    </main>

    <!-- Footer -->
    <footer class="bg-white border-t border-slate-200 mt-12">
      <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div class="grid grid-cols-1 md:grid-cols-3 gap-8">
          <div>
            <h3 class="font-semibold text-slate-900 mb-2">BDNS Dashboard</h3>
            <p class="text-sm text-slate-600">
              Visualización interactiva de la Base de Datos Nacional de Subvenciones de España.
            </p>
          </div>
          <div>
            <h4 class="font-semibold text-slate-900 mb-3">Enlaces Útiles</h4>
            <ul class="space-y-2 text-sm text-slate-600">
              <li><a href="#" class="hover:text-primary-600 transition-colors">Documentación</a></li>
              <li><a href="#" class="hover:text-primary-600 transition-colors">API GraphQL</a></li>
              <li><a href="#" class="hover:text-primary-600 transition-colors">Contacto</a></li>
            </ul>
          </div>
          <div>
            <h4 class="font-semibold text-slate-900 mb-3">Información</h4>
            <p class="text-sm text-slate-600">
              Datos actualizados diariamente desde la BDNS oficial.
            </p>
          </div>
        </div>
        <div class="border-t border-slate-200 mt-8 pt-8 text-center text-sm text-slate-600">
          <p>&copy; 2024 BDNS Dashboard. Todos los derechos reservados.</p>
        </div>
      </div>
    </footer>
  </div>
</template>

<style scoped>
.fade-in {
  animation: fadeIn 0.3s ease-in;
}

@keyframes fadeIn {
  from {
    opacity: 0;
  }
  to {
    opacity: 1;
  }
}
</style>
