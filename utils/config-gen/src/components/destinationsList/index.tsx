import { Header } from '@components/common/typography';
import DestinationCard from '@components/destinationCard';
import theme from '@css/theme';
import { IDestinationsListStore } from '@stores/destinationsList';
import { ReactComponent as Plus } from '@svg/plus.svg';
import { inject, observer } from 'mobx-react';
import * as React from 'react';
import { Link } from 'react-router-dom';
import { withTheme } from 'styled-components';

import { ButtonText, Container } from './styles';
import { IDestinationStore } from '@stores/destination';
import { IMessageStore } from '@stores/messages';

declare var LeaderLine: any;

export interface IDestinationsListProps {
  destinationsListStore?: IDestinationsListStore;
  theme: any;
  linesMap: any;
  messagesStore?: IMessageStore;
}

export interface IDestinationsListState {}

@inject('destinationsListStore', 'messagesStore')
@observer
class DestinationsList extends React.Component<
  IDestinationsListProps,
  IDestinationsListState
> {
  linesMap: any;

  constructor(props: IDestinationsListProps) {
    super(props);

    this.state = {};
    this.linesMap = props.linesMap;
  }

  onMouseEnter = (destination: any) => {
    Object.keys(this.linesMap).forEach(key => {
      let destId = key.split('-')[1];
      if (destId == destination.id) {
        this.linesMap[key].setOptions({
          color: this.props.theme.color.primary,
        });
      } else {
        this.linesMap[key].setOptions({
          size: 0.01,
        });
      }
    });
  };

  onMouseLeave = (destination: any) => {
    Object.values(this.linesMap).forEach((line: any) => {
      line.setOptions({
        color: this.props.theme.color.grey100,
        size: 4,
      });
    });
  };

  deleteDestination = (destination: IDestinationStore) => {
    const { destinationsListStore, messagesStore } = this.props;
    try {
      destinationsListStore!.deleteDestination(destination);
      messagesStore!.showSuccessMessage('Delete destination successful');
    } catch (error) {
      messagesStore!.showErrorMessage('Destination deletion failed');
    }
  };

  public render() {
    const { destinationsListStore } = this.props;
    const destinations =
      destinationsListStore && destinationsListStore.destinations;
    return (
      <Container style={{ zIndex: 1 }}>
        <Header color={theme.color.grey300} className="m-b-md">
          Destinations
        </Header>
        {!destinations || destinations.length === 0 ? (
          <div className="p-t-md">
            <DestinationCard destination={null} key={undefined} />
          </div>
        ) : (
          <div className="p-t-md">
            {destinations.map(destination => (
              <DestinationCard
                destination={destination}
                key={destination.name}
                onMouseEnter={this.onMouseEnter}
                onMouseLeave={this.onMouseLeave}
                onDelete={this.deleteDestination}
              />
            ))}
            <Link to="/destinations/setup" className="d-block p-t-sm">
              <Plus />
              <ButtonText>ADD DESTINATION</ButtonText>
            </Link>
          </div>
        )}
      </Container>
    );
  }
}

export default withTheme(DestinationsList);
