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