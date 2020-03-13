import { ConnectionsStore, IConnectionsStore } from '@stores/connections';
import {
  DestinationsListStore,
  IDestinationsListStore,
} from '@stores/destinationsList';
import {
  ISourceDefinitionsListStore,
  SourceDefinitionsListStore,
} from '@stores/sourceDefinitionsList';

import {
  DestinationDefsListStore,
  IDestinationDefsListStore,
} from './destinationDefsList';
import { ISourcesListStore, SourcesListStore } from './sourcesList';
import { IMessageStore, MessagesStore } from './messages';

export interface IRootStore {
  sourcesListStore: ISourcesListStore;
  destinationsListStore: IDestinationsListStore;
  sourceDefinitionsListStore: ISourceDefinitionsListStore;
  destinationDefsListStore: IDestinationDefsListStore;
  connectionsStore: IConnectionsStore;
  messagesStore: IMessageStore;
}

export class RootStore implements IRootStore {
  public sourcesListStore: ISourcesListStore;
  public destinationsListStore: IDestinationsListStore;
  public sourceDefinitionsListStore: ISourceDefinitionsListStore;
  public destinationDefsListStore: IDestinationDefsListStore;
  public connectionsStore: IConnectionsStore;
  public messagesStore: IMessageStore;

  constructor() {
    this.sourcesListStore = new SourcesListStore(this);
    this.destinationsListStore = new DestinationsListStore(this);
    this.sourceDefinitionsListStore = new SourceDefinitionsListStore(this);
    this.destinationDefsListStore = new DestinationDefsListStore(this);
    this.connectionsStore = new ConnectionsStore(this);
    this.messagesStore = new MessagesStore();
  }
}

export const stores = new RootStore();
