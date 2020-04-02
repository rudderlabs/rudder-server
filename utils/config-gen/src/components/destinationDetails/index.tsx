import { IDestinationsListStore } from '@stores/destinationsList';
import { ISourcesListStore } from '@stores/sourcesList';
import { inject, observer } from 'mobx-react';
import React, { Component } from 'react';
import { RouteComponentProps, withRouter } from 'react-router';

import DestinationView from './destinationView/index';
import SourceView from './sourcesView/index';
import { CardsView, Container, PageTitle, Spacing } from './styles';
import { ISourceStore } from '@stores/source';
import { IDestinationStore } from '@stores/destination';
import { IMessageStore } from '@stores/messages';

interface IConfiguredDestinationsProps extends RouteComponentProps<any> {
  sourcesListStore: ISourcesListStore;
  destinationsListStore: IDestinationsListStore;
  messagesStore: IMessageStore;
}

@inject('sourcesListStore', 'destinationsListStore', 'messagesStore')
@observer
class DestinationDetails extends Component<IConfiguredDestinationsProps, any> {
  constructor(props: IConfiguredDestinationsProps) {
    super(props);
    this.state = {
      destinationId: props.match && props.match.params.id,
    };
  }

  deleteConnection = async (
    source: ISourceStore,
    destination: IDestinationStore,
  ) => {
    const { destinationsListStore, messagesStore } = this.props;
    try {
      await destinationsListStore.rootStore.connectionsStore.removeConnections(
        source,
        destination,
      );
      messagesStore.showSuccessMessage('Connection deletion successful');
    } catch (error) {
      messagesStore.showErrorMessage('Failed to delete connection');
    }
  };

  deleteDestination = async (destination: IDestinationStore) => {
    const { destinationsListStore, messagesStore } = this.props;
    try {
      const isSuccess = await destinationsListStore.deleteDestination(
        destination,
      );
      console.log('isSuccess', isSuccess);
      if (!isSuccess) {
        throw Error('not successful');
      }
      messagesStore.showSuccessMessage('Delete destination successful');
      this.props.history.push(`/home`);
    } catch (error) {
      messagesStore.showErrorMessage('Failed to delete destination');
    }
  };

  public render() {
    const { destinationId } = this.state;
    const { destinationsListStore } = this.props;
    const { destinations } = destinationsListStore;
    const destination = destinations.find(
      destination => destination.id === destinationId,
    );
    if (destination) {
      return (
        <Container>
          <PageTitle>Destination Details</PageTitle>
          <CardsView>
            <Spacing>
              <DestinationView
                destination={destination}
                deleteDestination={this.deleteDestination}
              />
            </Spacing>
            <Spacing>
              <SourceView
                sources={destination!.sources}
                destination={destination}
                deleteConnection={this.deleteConnection}
              />
            </Spacing>
            <Spacing></Spacing>
          </CardsView>
        </Container>
      );
    }
    return null;
  }
}

export default withRouter(DestinationDetails);
