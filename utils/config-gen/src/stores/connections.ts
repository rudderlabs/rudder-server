import { action, observable, trace, autorun, set, toJS } from 'mobx';

import { IRootStore } from '.';
import { ISourceStore } from './source';
import { IDestinationStore } from './destination';

export interface IConnectionsStore {
  connections: ISourceConnections;
  rootStore: IRootStore;
  setConnections(sources: ISourceStore[]): void;
  removeConnections(source: ISourceStore, destination: IDestinationStore): void;
  loadAndSave(): any;
  loadImportedFile(sources: any): any;
  returnWithoutRootStore(): any;
}

export interface ISourceConnections {
  [key: string]: string[];
}

function autoSave(store: any, save: any) {
  let firstRun = true;
  autorun(() => {
    const connectionsStore = toJS(store);
    delete connectionsStore.rootStore;
    const json = JSON.stringify(connectionsStore);
    if (!firstRun) {
      save(json);
    }
    firstRun = false;
  });
}

export class ConnectionsStore implements IConnectionsStore {
  @observable connections: ISourceConnections = {};
  rootStore: IRootStore;

  constructor(rootStore: IRootStore) {
    this.rootStore = rootStore;
    this.loadAndSave();
  }

  public loadAndSave() {
    this.load();
    autoSave(this, this.save.bind(this));
  }

  public returnWithoutRootStore() {
    const connectionsStore = toJS(this);
    delete connectionsStore.rootStore;
    return connectionsStore;
  }

  public load() {
    const connectionsStore = localStorage.getItem('connectionsStore');
    if (connectionsStore) {
      const store: IConnectionsStore = JSON.parse(connectionsStore);
      set(this, store);
    }
  }

  public loadImportedFile(connections: any) {
    this.connections = connections[0];
  }

  public save(json: string) {
    localStorage.setItem('connectionsStore', json);
  }

  @action.bound
  public async setConnections(sources: ISourceStore[]) {
    let connections: ISourceConnections = {};
    sources.forEach(source => {
      connections[source.id] = source.destinations.map(dest => dest.id);
    });
    this.connections = connections;
  }

  @action.bound
  public async removeConnections(
    source: ISourceStore,
    destination: IDestinationStore,
  ) {
    const sourceIds = [source.id];
    let destinations: any = this.connections[source.id];
    let remainingDestinations = destinations.filter((destId: any) => {
      return destId != destination.id;
    });
    if (remainingDestinations.length > 0) {
      this.connections[source.id] = remainingDestinations;
    } else {
      delete this.connections[source.id];
    }
  }
}
