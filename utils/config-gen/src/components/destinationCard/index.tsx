import { LabelDiv, Text } from '@components/common/typography';
import theme from '@css/theme';
import { IDestinationStore } from '@stores/destination';
import { ReactComponent as Check } from '@svg/check.svg';
import { ReactComponent as Plus } from '@svg/plus.svg';
import { observer } from 'mobx-react';
import * as React from 'react';
import { Link } from 'react-router-dom';

import DestinationIcon from '../icons/destinationIcon';
import {
  ButtonText,
  Content,
  EmptyStyledCard,
  EnabledText,
  StyledCard,
} from './styles';
import { Flex } from '@components/common/misc';

export interface IDestinationCardProps {
  destination: IDestinationStore | null;
  onMouseEnter?: (destination: any) => void;
  onMouseLeave?: (destination: any) => void;
  onDelete?: (destination: any) => void;
}

export interface IDestinationCardState {}

export const DestiantionFieldNamesMap: any = {
  trackingId: 'Tracking ID',
  apiKey: 'API Key',
};

@observer
export default class DestinationCard extends React.Component<
  IDestinationCardProps,
  IDestinationCardState
> {
  constructor(props: IDestinationCardProps) {
    super(props);

    this.state = {};
  }

  onMouseEnter = () => {
    this.props.onMouseEnter!(this.props.destination);
  };

  onMouseLeave = () => {
    this.props.onMouseLeave!(this.props.destination);
  };

  deleteSource = (e: any) => {
    e.preventDefault();
    this.props.onDelete!(this.props.destination);
  };

  public render() {
    const { destination } = this.props;
    if (destination === null) {
      return (
        <Link to="/destinations/setup">
          <EmptyStyledCard>
            <Content>
              <Plus />
              <ButtonText>ADD DESTINATION</ButtonText>
            </Content>
          </EmptyStyledCard>
        </Link>
      );
    } else {
      return (
        <Link to={`/destinations/${destination!.id}`}>
          <StyledCard
            id={`destination-${destination!.id}`}
            onMouseEnter={this.onMouseEnter}
            onMouseLeave={this.onMouseLeave}
          >
            <Flex
              flexDirection="row"
              style={{ width: '1px', paddingLeft: '35px' }}
              id={`fake-destination-${destination!.id}`}
            />
            <div
              style={{
                height: '41px',
                width: '41px',
                display: 'flex',
                justifyContent: 'center',
                alignItems: 'center',
              }}
            >
              <DestinationIcon
                destination={destination.destinationDefinition.name}
                height={theme.iconSize.large}
                width={theme.iconSize.large}
              />
            </div>
            <Content>
              <Flex flexDirection="row" spaceBetween>
                <LabelDiv>{destination.name}</LabelDiv>
              </Flex>
              <Flex flexDirection="row" spaceBetween />
              {destination.enabled && (
                <>
                  <Check />
                  <EnabledText color={theme.color.green}>Enabled</EnabledText>
                </>
              )}
              {!destination.enabled && (
                <>
                  <Text color={theme.color.grey300}>Disabled</Text>
                </>
              )}
            </Content>
          </StyledCard>
        </Link>
      );
    }
  }
}
